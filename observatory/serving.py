"""Publish validated dbt Gold to PostgreSQL and expose a read-only serving contract."""
from __future__ import annotations
import argparse
import json
import os
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from observatory.warehouse import GOLD_TABLES, validate_dbt_results

DATA = ROOT / 'data'
PUBLICATION_LOCK = 71930428
PROMOTION_LOCK = 71930429
PRIMARY_KEYS = {
    'dim_location': ['id'], 'dim_month': ['year_month'],
    'fact_climate_monthly': ['site', 'year', 'month'], 'fact_infrastructure': ['site'],
    'source_audit': ['dataset', 'year'], 'mart_monthly_anomalies': ['site', 'year', 'month'],
    'mart_water_year': ['site', 'water_year'], 'mart_site_trends': ['site'],
    'mart_spatial_rainfall': ['year', 'month'],
}


def load_environment():
    path = Path(os.environ.get('OBSERVATORY_ENV_FILE', ROOT / '.env.morocco'))
    if path.exists():
        for line in path.read_text(encoding='utf-8-sig').splitlines():
            if line.strip() and not line.lstrip().startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key.strip(), value.strip())


def connection(role='reader', database=None, autocommit=False):
    load_environment()
    user, key = {'reader': ('bi_reader', 'BI_READER_PASSWORD'),
                 'writer': ('pipeline_writer', 'PIPELINE_WRITER_PASSWORD'),
                 'admin': (os.environ.get('POSTGRES_USER', 'morocco_admin'), 'POSTGRES_PASSWORD')}[role]
    return psycopg.connect(host=os.environ.get('SERVING_DB_HOST', '127.0.0.1'),
                           port=int(os.environ.get('SERVING_DB_PORT', '5432')),
                           dbname=database or os.environ.get('SERVING_DB_NAME', 'observatory'),
                           user=user, password=os.environ[key], connect_timeout=5,
                           sslmode=os.environ.get('PGSSLMODE', 'prefer'), autocommit=autocommit)


def postgres_type(kind):
    kind = kind.lower()
    if kind in {'int', 'integer', 'bigint', 'smallint', 'tinyint', 'long', 'hugeint', 'ubigint', 'uinteger'}:
        return 'BIGINT'
    if kind in {'double', 'float', 'real'} or kind.startswith(('decimal', 'numeric')):
        return 'DOUBLE PRECISION'
    if kind == 'boolean':
        return 'BOOLEAN'
    if kind == 'date':
        return 'DATE'
    if kind.startswith('timestamp'):
        return 'TIMESTAMP'
    return 'TEXT'


def bootstrap():
    load_environment()
    database = os.environ.get('SERVING_DB_NAME', 'observatory')
    with connection('admin', database='postgres', autocommit=True) as conn:
        with conn.cursor() as cur:
            for role, key in [('pipeline_writer', 'PIPELINE_WRITER_PASSWORD'), ('bi_reader', 'BI_READER_PASSWORD')]:
                if not cur.execute('select 1 from pg_roles where rolname=%s', (role,)).fetchone():
                    cur.execute(sql.SQL('create role {} login password {}').format(sql.Identifier(role), sql.Literal(os.environ[key])))
            if not cur.execute('select 1 from pg_database where datname=%s', (database,)).fetchone():
                cur.execute(sql.SQL('create database {}').format(sql.Identifier(database)))
    with connection('admin') as conn:
        conn.execute('create schema if not exists gold authorization pipeline_writer')
        conn.execute('grant usage on schema gold to bi_reader')
        conn.execute('alter default privileges for role pipeline_writer in schema gold grant select on tables to bi_reader')
    print('PostgreSQL serving database and reader/writer roles are ready; credentials were not printed.')


@contextmanager
def publication_session():
    # Hold a session lock across both the database transaction and export promotion.
    with connection('writer', autocommit=True) as conn:
        conn.execute('select pg_advisory_lock(%s)', (PROMOTION_LOCK,))
        yield conn


def current_promotion(conn):
    if not conn.execute("select to_regclass('gold.dashboard_metadata')").fetchone()[0]:
        return None
    row = conn.execute("select payload from gold.dashboard_metadata where name = 'promotion'").fetchone()
    if row:
        marker = conn.execute('select publication_id, table_counts from gold.publication where singleton').fetchone()
        if not marker or marker[0] != row[0]['publication_id'] or marker[1] != row[0]['tables']:
            raise ValueError('PostgreSQL publication marker and promotion receipt disagree')
    return row[0] if row else None


def publish_tables(staged, snapshot, assets, build_results, publication_id=None, *, promotion=None):
    with publication_session() as conn:
        return commit_tables(conn, staged, snapshot, assets, build_results, publication_id, promotion)


def commit_tables(conn, staged, snapshot, assets, build_results, publication_id=None, promotion=None):
    validate_dbt_results(build_results, 'duckdb' if build_results.get('args', {}).get('target') == 'local' else 'spark')
    if set(staged) != set(GOLD_TABLES):
        raise ValueError('All nine Gold models are required in one publication')
    publication_id = publication_id or uuid.uuid4().hex
    published_at = datetime.now(timezone.utc).isoformat()
    snapshot = {**snapshot, 'meta': {**snapshot['meta'], 'publication_id': publication_id, 'published_at': published_at,
                                   'serving_source': 'PostgreSQL', 'dbt_invocation_id': build_results['metadata']['invocation_id']}}
    counts = {name: len(value['rows']) for name, value in staged.items()}
    if any(count == 0 for count in counts.values()):
        raise ValueError('Empty Gold models cannot replace a published dashboard')
    previous = current_promotion(conn)
    if previous and not previous['complete']:
        raise ValueError('Repair the pending publication before committing a new one')
    if promotion and (promotion['publication_id'] != publication_id or promotion['tables'] != counts):
        raise ValueError('Promotion receipt does not match the staged Gold tables')
    with conn.transaction(), conn.cursor() as cur:
        cur.execute('select pg_advisory_xact_lock(%s)', (PUBLICATION_LOCK,))
        # Drop dependent facts before their referenced dimensions; no CASCADE.
        for table in reversed(GOLD_TABLES):
            cur.execute(sql.SQL('drop table if exists gold.{}').format(sql.Identifier(table)))
        for table, value in staged.items():
            definitions = sql.SQL(',').join(sql.SQL('{} {}').format(sql.Identifier(c), sql.SQL(t))
                                           for c, t in zip(value['columns'], value['types']))
            if any(t not in {'BIGINT', 'DOUBLE PRECISION', 'BOOLEAN', 'DATE', 'TIMESTAMP', 'TEXT'} for t in value['types']):
                raise ValueError('Unsupported serving column type')
            cur.execute(sql.SQL('create table gold.{} ({})').format(sql.Identifier(table), definitions))
            with cur.copy(sql.SQL('copy gold.{} ({}) from stdin').format(sql.Identifier(table),
                          sql.SQL(',').join(map(sql.Identifier, value['columns'])))) as output:
                for row in value['rows']:
                    output.write_row(row)
            cur.execute(sql.SQL('alter table gold.{} add primary key ({})').format(sql.Identifier(table),
                              sql.SQL(',').join(map(sql.Identifier, PRIMARY_KEYS[table]))))
            if cur.execute(sql.SQL('select count(*) from gold.{}').format(sql.Identifier(table))).fetchone()[0] != counts[table]:
                raise ValueError('PostgreSQL row-count reconciliation failed: ' + table)
        for table in ['fact_climate_monthly', 'fact_infrastructure', 'mart_monthly_anomalies', 'mart_water_year', 'mart_site_trends']:
            cur.execute(sql.SQL('alter table gold.{} add foreign key (site) references gold.dim_location(id)').format(sql.Identifier(table)))
        for table in ['fact_climate_monthly', 'mart_monthly_anomalies', 'mart_spatial_rainfall']:
            cur.execute(sql.SQL('alter table gold.{} add foreign key (year_month) references gold.dim_month(year_month)').format(sql.Identifier(table)))
        cur.execute('create table if not exists gold.dashboard_metadata (name text primary key, payload jsonb not null)')
        cur.execute('create table if not exists gold.dashboard_assets (name text primary key, payload jsonb not null)')
        cur.execute('create table if not exists gold.publication (singleton boolean primary key check(singleton), publication_id text not null, published_at timestamptz not null, table_counts jsonb not null)')
        for name, payload in [('snapshot', snapshot), ('dbt_run_results', build_results)]:
            cur.execute('insert into gold.dashboard_metadata values (%s,%s) on conflict(name) do update set payload=excluded.payload', (name, Jsonb(payload)))
        if promotion:
            cur.execute("insert into gold.dashboard_metadata values ('promotion',%s) on conflict(name) do update set payload=excluded.payload",
                        (Jsonb(promotion),))
        else:
            cur.execute("delete from gold.dashboard_metadata where name = 'promotion'")
        cur.execute('delete from gold.dashboard_assets')
        for name, payload in assets.items():
            cur.execute('insert into gold.dashboard_assets values (%s,%s)', (name, Jsonb(payload)))
        cur.execute('insert into gold.publication values (true,%s,%s,%s) on conflict(singleton) do update set publication_id=excluded.publication_id,published_at=excluded.published_at,table_counts=excluded.table_counts',
                    (publication_id, published_at, Jsonb(counts)))
        cur.execute('grant select on all tables in schema gold to bi_reader')
    return {'publication_id': publication_id, 'published_at': published_at, 'tables': counts}


def publish_local():
    import duckdb
    results = json.loads((DATA / 'metadata/dbt_run_results.json').read_text())
    validate_dbt_results(results, 'duckdb')
    provenance = json.loads((DATA / 'gold/provenance.json').read_text())
    if provenance['dbt_invocation_id'] != results['metadata']['invocation_id']:
        raise ValueError('Gold metadata and the successful dbt build do not match; export Gold first')
    staged = {}
    with duckdb.connect(str(DATA / 'warehouse/observatory.duckdb'), read_only=True) as con:
        for table in GOLD_TABLES:
            description = con.execute(f'describe gold.{table}').fetchall()
            rows = con.execute(f'select * from gold.{table}').fetchall()
            staged[table] = {'columns': [row[0] for row in description],
                             'types': [postgres_type(row[1]) for row in description], 'rows': rows}
    snapshot = json.loads((DATA / 'gold/snapshot.json').read_text(encoding='utf-8'))
    assets = {name: json.loads((DATA / 'gold' / name).read_text(encoding='utf-8')) for name in ['osm.geojson', 'footprint.geojson', 'provenance.json']}
    receipt = publish_tables(staged, snapshot, assets, results)
    (DATA / 'metadata/postgres_publication.json').write_text(json.dumps(receipt, indent=2), encoding='utf-8')
    print(json.dumps(receipt, indent=2))


def verify():
    with connection() as conn:
        conn.execute('set transaction isolation level repeatable read read only')
        conn.execute('select pg_advisory_xact_lock_shared(%s)', (PUBLICATION_LOCK,))
        marker = conn.execute('select publication_id,table_counts from gold.publication where singleton').fetchone()
        if not marker:
            raise ValueError('No committed PostgreSQL Gold publication')
        counts = {}
        for table in GOLD_TABLES:
            counts[table] = conn.execute(sql.SQL('select count(*) from gold.{}').format(sql.Identifier(table))).fetchone()[0]
            if conn.execute('select has_table_privilege(current_user,%s,\'INSERT\')', ('gold.' + table,)).fetchone()[0]:
                raise ValueError('Dashboard role must be read-only')
        if counts != marker[1]:
            raise ValueError('Serving counts disagree with the publication marker')
    result = {'verified_at': datetime.now(timezone.utc).isoformat(), 'publication_id': marker[0],
              'tables': counts, 'reader_role': 'bi_reader', 'read_only_verified': True}
    (DATA / 'metadata/postgres_verification.json').write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['bootstrap', 'publish-local', 'verify'])
    command = parser.parse_args().command
    {'bootstrap': bootstrap, 'publish-local': publish_local, 'verify': verify}[command]()
