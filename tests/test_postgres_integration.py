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
