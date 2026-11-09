"""Run the real dbt test SQL against small relations with intentional defects."""
from pathlib import Path

import duckdb
import pytest
from jinja2 import Environment, StrictUndefined

TEST_SQL = Path(__file__).resolve().parents[1] / 'dbt/tests'


def failing_rows(con, name):
    template = Environment(undefined=StrictUndefined).from_string((TEST_SQL / f'{name}.sql').read_text())
    return con.execute(template.render(ref=lambda model: model)).fetchall()


@pytest.mark.parametrize('defect', [None, 'missing_month', 'missing_provider', 'missing_year', 'extra_year'])
def test_gold_calendar_requires_both_providers_and_complete_years(defect):
    with duckdb.connect() as con:
        con.execute('create table source_audit (year int, dataset varchar)')
        con.execute("insert into source_audit values (2024, 'ERA5-Land'), (2024, 'CHIRPS v3')")
        con.execute('create table dim_month as select 2024 as year, month from range(1, 13) as t(month)')
        if defect == 'missing_month':
            con.execute('delete from dim_month where month = 4')
        elif defect == 'missing_provider':
            con.execute("delete from source_audit where dataset = 'ERA5-Land'")
        elif defect == 'missing_year':
            con.execute('delete from dim_month')
        elif defect == 'extra_year':
            con.execute('update dim_month set year = 2025')
        expected = [(2024,), (2025,)] if defect == 'extra_year' else [(2024,)] if defect else []
        assert sorted(failing_rows(con, 'calendar_source_coverage')) == expected


def test_hourly_reconciliation_detects_compensating_errors_and_extra_gold():
    with duckdb.connect() as con:
        con.execute("""create table stg_climate_hourly as
                     select 'rabat' as site, 2024 as year, month
                     from (values (1), (1), (2), (2)) as t(month)""")
        con.execute("""create table fact_climate_monthly as
                     select 'rabat' as site, 2024 as year, month, 2 as hours
                     from (values (1), (2)) as t(month)""")
        assert failing_rows(con, 'hourly_reconciliation') == []
        con.execute('update fact_climate_monthly set hours = case when month = 1 then 1 else 3 end')
        con.execute("insert into fact_climate_monthly values ('fes', 2024, 1, 2)")
        assert sorted(failing_rows(con, 'hourly_reconciliation')) == [
            ('fes', 2024, 1), ('rabat', 2024, 1), ('rabat', 2024, 2),
        ]


@pytest.mark.parametrize(('table', 'valid_values', 'bad_column', 'bad_value'), [
    ('chirps_grid', [2024, 2, 34, -6, 12], 'rainfall_mm', -1),
    ('chirps_sites', ['rabat', 2024, 2, 12, 34, -6], 'month', 13),
    ('era5_hourly', ['rabat', '2024-02-01T00:00', 18, 0.4], 'soil_moisture', 1.2),
    ('locations', ['rabat', 'Rabat', 'Coastal', 34, -6, 34, -6], 'lat', 91),
    ('osm_services', ['rabat', 'node/1', 'water', 'Point', 34, -6], 'category', 'unknown'),
    ('osm_roads', ['rabat', 5], 'road_km', float('inf')),
    ('source_audit', ['ERA5-Land', 2024, 'archive', 50, 'a' * 64], 'sha256', 'z' * 64),
])
def test_silver_contracts_accept_valid_and_reject_invalid_records(table, valid_values, bad_column, bad_value):
    from observatory.contracts import TABLES, cast_query, dedupe_query

    fields = TABLES[table][0]
    invalid_values = valid_values.copy()
    invalid_values[list(fields).index(bad_column)] = bad_value
    columns = [f'{name} {"varchar" if kind == "string" else kind}' for name, kind in fields.values()]
    with duckdb.connect() as con:
        con.execute('create schema bronze')
        con.execute(f'create table bronze.{table} ({", ".join(columns)}, _ingested_at int, _source_file varchar, _row_hash varchar)')
        placeholders = ', '.join(['?'] * (len(valid_values) + 3))
        con.executemany(f'insert into bronze.{table} values ({placeholders})', [
            [*valid_values, 1, 'source-a', 'good'],
            [*invalid_values, 2, 'source-b', 'bad'],
        ])
        con.execute('create temp view candidates as ' + cast_query(table, 'duckdb'))
        assert [row[-1] for row in con.execute(dedupe_query(table)).fetchall()] == ['good']
