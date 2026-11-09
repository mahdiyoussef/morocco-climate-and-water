"""Real PostgreSQL publication, read-only dashboard loading and rollback checks."""
import copy
import csv
import json
import os
import uuid
from pathlib import Path

import duckdb
import psycopg
import pytest
from psycopg import sql
from observatory.serving import bootstrap, connection, postgres_type, publish_tables
from observatory.warehouse import GOLD_TABLES
from dashboard.postgres_backend import load_postgres_bundle

pytestmark = pytest.mark.skipif(os.environ.get('OBSERVATORY_POSTGRES_TESTS') != '1', reason='Set OBSERVATORY_POSTGRES_TESTS=1 to run against local PostgreSQL')
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope='module')
def published():
    database = 'observatory_test_' + uuid.uuid4().hex[:12]
    previous = os.environ.get('SERVING_DB_NAME')
    os.environ['SERVING_DB_NAME'] = database
    bootstrap()
    staged = {}
    with duckdb.connect() as con:
        for table in GOLD_TABLES:
            path = ROOT / 'data/gold' / f'{table}.csv'
            with path.open(encoding='utf-8') as stream:
                names = csv.DictReader(stream).fieldnames
            types = {name: 'VARCHAR' for name in ['site', 'id', 'year_month', 'sha256', 'path'] if name in names}
            view = con.read_csv(str(path), header=True, dtype=types)
            staged[table] = {'columns': view.columns, 'types': [postgres_type(str(kind)) for kind in view.types], 'rows': view.fetchall()}
    snapshot = json.loads((ROOT / 'data/gold/snapshot.json').read_text(encoding='utf-8'))
    assets = {name: json.loads((ROOT / 'data/gold' / name).read_text(encoding='utf-8')) for name in ['osm.geojson', 'footprint.geojson', 'provenance.json']}
    build = json.loads((ROOT / 'data/metadata/dbt_run_results.json').read_text())
    receipt = publish_tables(staged, snapshot, assets, build)
    try:
        yield staged, snapshot, assets, build, receipt
    finally:
        # Only the uniquely named database created by this fixture is removed.
        assert database.startswith('observatory_test_') and len(database) == len('observatory_test_') + 12
        with connection('admin', database='postgres', autocommit=True) as admin:
            admin.execute(sql.SQL('drop database {} with (force)').format(sql.Identifier(database)))
        if previous is None:
            os.environ.pop('SERVING_DB_NAME', None)
        else:
            os.environ['SERVING_DB_NAME'] = previous


def test_dashboard_reads_real_postgresql_gold(published):
    bundle = load_postgres_bundle()
    assert bundle['backend'] == 'postgres'
    assert bundle['publication_id'] == published[-1]['publication_id']
    assert len(bundle['fact']) == 1440 and len(bundle['marts']) == 4
    assert len(bundle['assets']['osm.geojson']['features']) == 8652
    with connection() as conn:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute('delete from gold.fact_climate_monthly where false')
        conn.rollback()


def test_failed_publication_keeps_previous_snapshot(published):
    staged, snapshot, assets, build, receipt = published
    damaged = copy.deepcopy(staged)
    damaged['fact_climate_monthly']['rows'].append(damaged['fact_climate_monthly']['rows'][0])
    with pytest.raises(psycopg.errors.UniqueViolation):
        publish_tables(damaged, snapshot, assets, build)
    with connection() as conn:
        assert conn.execute('select publication_id from gold.publication where singleton').fetchone()[0] == receipt['publication_id']
        assert conn.execute('select count(*) from gold.fact_climate_monthly').fetchone()[0] == 1440


@pytest.mark.skipif(os.environ.get('OBSERVATORY_GUARDRAIL_INTEGRATION') != '1',
                    reason='Requires local Spark and a disposable MinIO endpoint')
def test_guardrail_service_recovery(published, tmp_path, monkeypatch):
    from jinja2 import Environment
    from pyspark.sql import SparkSession
    from observatory import promotion, publish
    from observatory.serving import PROMOTION_LOCK, current_promotion, publication_session
    from observatory.storage import client, put_file

    s3 = client()
    s3.create_bucket(Bucket='landing')
    s3.create_bucket(Bucket='exports')
    spark = (SparkSession.builder.master('local[1]').appName('guardrail-integration')
             .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', 1).getOrCreate())
    try:
        parquet = tmp_path / 'spark-audit'
        spark.createDataFrame([(2024, 'CHIRPS v3'), (2024, 'ERA5-Land')], ['year', 'dataset']).write.parquet(str(parquet))
        original = next(parquet.glob('*.parquet'))
        entry = put_file(s3, original, 'landing', 'fixture/audit.parquet')
        copy_path = tmp_path / 'audit.parquet'
        copy_path.write_bytes(promotion.verified_body(s3, 'landing', entry))
        spark.read.parquet(str(copy_path)).createOrReplaceTempView('source_audit')
        spark.range(1, 13).selectExpr('2024 as year', 'id as month').createOrReplaceTempView('dim_month')
        query = (ROOT / 'dbt/tests/calendar_source_coverage.sql').read_text()
        sql_text = Environment().from_string(query).render(ref=lambda name: name)
        assert spark.sql(sql_text).count() == 0
        spark.range(1, 12).selectExpr('2024 as year', 'id as month').createOrReplaceTempView('dim_month')
        assert [row.year for row in spark.sql(sql_text).collect()] == [2024]
    finally:
        spark.stop()

    staged, snapshot, assets, build, _ = published
    version = 'guardrail-integration'
    counts = {table: len(value['rows']) for table, value in staged.items()}
    stage = tmp_path / 'exports'
    stage.mkdir()
    publish.write_csvs(stage, staged)
    snapshot = {**snapshot, 'meta': {**snapshot['meta'], 'publication_id': version}}
    for name, payload in {**assets, 'snapshot.json': snapshot, 'dbt_run_results.json': build,
                          'provenance.json': {'publication_id': version, 'tables': counts}}.items():
        (stage / name).write_text(json.dumps(payload))
    files = [put_file(s3, path, 'exports', f'observatory/runs/{version}/{path.name}') for path in stage.iterdir()]
    receipt = {'publication_id': version, 'tables': counts, 'files': files, 'complete': False,
               'logical_date': '2024-01-01T00:00:00+00:00', 'published_at': '2024-01-01T00:00:00+00:00'}

    publish_tables(staged, snapshot, assets, build, version, promotion=receipt)
    monkeypatch.setattr(publish, 'DATA', tmp_path / 'data')
    monkeypatch.setattr(publish, 'client', lambda: s3)
    actual_put = s3.put_object
    with publication_session() as conn:
        with connection('writer', autocommit=True) as concurrent:
            assert not concurrent.execute('select pg_try_advisory_lock(%s)', (PROMOTION_LOCK,)).fetchone()[0]
        assert current_promotion(conn) == receipt
        with monkeypatch.context() as inject:
            def fail_pointer(**kwargs):
                if kwargs['Key'] == promotion.CURRENT:
                    raise OSError('injected pointer failure')
                return actual_put(**kwargs)
            inject.setattr(s3, 'put_object', fail_pointer)
            with pytest.raises(OSError, match='injected pointer'):
                promotion.finish(conn, s3, publish.DATA, receipt)
        assert not current_promotion(conn)['complete']
    assert load_postgres_bundle()['publication_id'] == version

    publish.repair()
    publish.repair()
    with publication_session() as conn:
        assert current_promotion(conn)['complete']
    pointer = json.loads(s3.get_object(Bucket='exports', Key=promotion.CURRENT)['Body'].read())
    local = json.loads((publish.DATA / 'gold/snapshot.json').read_text())
    assert pointer['publication_id'] == local['meta']['publication_id'] == version
    assert not (publish.DATA / 'gold/.promotion-pending').exists()
