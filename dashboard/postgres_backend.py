"""Load all dashboard facts, metadata and map geometry from one PostgreSQL publication."""
import pandas as pd
from psycopg import sql

from observatory.serving import connection, PUBLICATION_LOCK
from observatory.warehouse import GOLD_TABLES
from dashboard.data import build_bundle, DEFAULT_DATA_ROOT


def load_postgres_bundle():
    with connection() as conn:
        conn.execute('set transaction isolation level repeatable read read only')
        conn.execute('select pg_advisory_xact_lock_shared(%s)', (PUBLICATION_LOCK,))
        marker = conn.execute('select publication_id,published_at,table_counts from gold.publication where singleton').fetchone()
        if marker is None:
            raise ValueError('Publish a tested Gold dataset to PostgreSQL before opening the dashboard')
        tables = {}
        for table in GOLD_TABLES:
            cursor = conn.execute(sql.SQL('select * from gold.{}').format(sql.Identifier(table)))
            tables[table] = pd.DataFrame(cursor.fetchall(), columns=[column.name for column in cursor.description])
            if len(tables[table]) != marker[2][table]:
                raise ValueError('PostgreSQL publication row counts do not reconcile')
        metadata = dict(conn.execute('select name,payload from gold.dashboard_metadata').fetchall())
        assets = dict(conn.execute('select name,payload from gold.dashboard_assets').fetchall())
    bundle = build_bundle(tables, metadata['snapshot'], {'dbt': metadata['dbt_run_results']}, DEFAULT_DATA_ROOT, assets)
    bundle.update(backend='postgres', publication_id=marker[0], published_at=marker[1].isoformat())
    return bundle
