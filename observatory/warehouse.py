"""Normalize provider files, verify Bronze/Silver locally, execute dbt, and export Gold.

The local DuckDB target verifies SQL and data without Docker. Production Bronze and
Silver are Spark/Delta on MinIO; it is deliberately not reported as a cluster test.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from observatory.contracts import TABLES, cast_query, dedupe_query, validity

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data'
DB_PATH = DATA / 'warehouse/observatory.duckdb'
GOLD_TABLES = ['dim_location', 'dim_month', 'fact_climate_monthly', 'fact_infrastructure', 'source_audit',
               'mart_monthly_anomalies', 'mart_water_year', 'mart_site_trends', 'mart_spatial_rainfall']


def record_osm_provenance():
    from datetime import datetime, timezone
    records = []
    for filename, url in [('morocco.osm.pbf', 'https://download.geofabrik.de/africa/morocco-latest.osm.pbf'),
                          ('morocco.poly', 'https://download.geofabrik.de/africa/morocco.poly')]:
        path = DATA / 'raw/osm' / filename
        digest = hashlib.sha256()
        with path.open('rb') as stream:
            for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
                digest.update(block)
        records.append({'path': path.relative_to(ROOT).as_posix(), 'url': url,
                        'bytes': path.stat().st_size, 'sha256': digest.hexdigest()})
    result = {'dataset': 'OpenStreetMap / Geofabrik', 'verified_at': datetime.now(timezone.utc).isoformat(), 'files': records}
    (DATA / 'metadata/osm_downloads.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    return result


def validate_dbt_results(results, adapter):
    """Require every Gold model and test; exposures are lineage-only no-op nodes."""
    expected_target = {'duckdb': 'local', 'spark': 'spark'}[adapter]
    if results.get('args', {}).get('target') != expected_target or results.get('args', {}).get('which') != 'build':
        raise ValueError('A complete dbt build for the expected target is required')
    nodes = [row for row in results.get('results', []) if not row['unique_id'].startswith('exposure.')]
    models = {row['unique_id'].rsplit('.', 1)[-1] for row in nodes if row['unique_id'].startswith('model.')}
    tests = [row for row in nodes if row['unique_id'].startswith('test.')]
    if not set(GOLD_TABLES) <= models or not tests or any(row['status'] not in ['success', 'pass'] for row in nodes):
        raise ValueError('All Gold models and dbt tests must pass before publication')


def prepare_sources(from_landing=False):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from observatory.pipeline import SITES, PROCESSED, footprint, read_rain, read_era5, osm_context
    if from_landing:
        from observatory.storage import restore_raw
        restore_raw()
    meta = json.loads((DATA / 'metadata/climate_downloads.json').read_text())
    if not meta.get('minimum_size_met') or not meta.get('final_verification_utc'):
        raise ValueError('Climate acquisition must be verified and exceed 1 GiB before normalization')
    years = list(range(meta['years'][0], meta['years'][1] + 1))
    PROCESSED.mkdir(parents=True, exist_ok=True)
    if from_landing or not (PROCESSED / 'chirps_sites.parquet').exists():
        read_rain(years, footprint())
    if from_landing or not list((PROCESSED / 'era5_hourly').rglob('*.parquet')):
        read_era5(years)
    if from_landing or not (PROCESSED / 'osm_services.geojson').exists() or not (PROCESSED / 'osm_roads.parquet').exists():
        osm_context()
    bronze = DATA / 'bronze'
    for table, directory in [('chirps_grid', 'chirps_grid'), ('era5_hourly', 'era5_hourly')]:
        shutil.copytree(PROCESSED / directory, bronze / table, dirs_exist_ok=True)
    def save(table, rows=None, source=None):
        target = bronze / table / 'part.parquet'
        target.parent.mkdir(parents=True, exist_ok=True)
        if source:
            shutil.copyfile(source, target)
        else:
            pq.write_table(pa.Table.from_pylist(rows), target, compression='zstd')
    save('chirps_sites', source=PROCESSED / 'chirps_sites.parquet')
    save('osm_roads', source=PROCESSED / 'osm_roads.parquet')
    payload = json.loads((DATA / f'raw/era5/era5_land_{years[0]}.json').read_text())
    save('locations', [{**site, 'era5_lat': p['latitude'], 'era5_lon': p['longitude']} for site, p in zip(SITES, payload)])
    features = json.loads((PROCESSED / 'osm_services.geojson').read_text(encoding='utf-8'))['features']
    rows = []
    for feature in features:
        lon, lat = feature['geometry']['coordinates']
        p = feature['properties']
        rows += [{'site': site, 'osm_id': p['osm_id'], 'category': p['category'], 'name': p['name'], 'lat': lat, 'lon': lon} for site in p['sites']]
    save('osm_services', rows)
    era = json.loads((DATA / 'metadata/era5_downloads.json').read_text())
    audit = [{'dataset': 'CHIRPS v3', 'year': r['year'], 'path': r['path'], 'bytes': r['bytes'], 'sha256': r['sha256']} for r in meta['files']]
    audit += [{'dataset': 'ERA5-Land', 'year': int(Path(path).stem.rsplit('_', 1)[1]), 'path': path, 'bytes': r['bytes'], 'sha256': r['sha256']} for path, r in era['files'].items()]
    for dataset in ('CHIRPS v3', 'ERA5-Land'):
        recorded_years = sorted(row['year'] for row in audit if row['dataset'] == dataset)
        if recorded_years != years:
            raise ValueError(f'{dataset}: source audit must record each requested year exactly once')
    save('source_audit', audit)
    manifest = {'tables': {}, 'source_manifest': 'metadata/climate_downloads.json'}
    for table in TABLES:
        files = sorted((bronze / table).rglob('*.parquet'))
        if not files:
            raise ValueError(f'{table}: no normalized Parquet files produced')
        manifest['tables'][table] = [{'path': str(p.relative_to(DATA)).replace('\\', '/'), 'bytes': p.stat().st_size,
                                      'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in files]
    (DATA / 'metadata/bronze_inputs.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    from datetime import datetime, timezone
    record_osm_provenance()
    extraction = {'meta': {'title': 'Morocco Climate & Water Observatory', 'dashboard': 'Power BI + Streamlit',
                           'built_at': datetime.now(timezone.utc).isoformat(), 'years': years,
                           'reference_year': years[0], 'baseline_period': [2015, 2019], 'site_count': len(SITES),
                           'site_ids': [site['id'] for site in SITES],
                           'site_list_sha256': hashlib.sha256((ROOT / 'observatory/sites.json').read_bytes()).hexdigest(),
                           'chirps_grid_records': sum(pq.read_metadata(p).num_rows for p in (bronze / 'chirps_grid').rglob('*.parquet')),
                           'era5_hourly_records': sum(pq.read_metadata(p).num_rows for p in (bronze / 'era5_hourly').rglob('*.parquet')),
                           'osm': json.loads((PROCESSED / 'osm_stats.json').read_text()),
                           'raw_bytes': sum(p.stat().st_size for p in (DATA / 'raw').rglob('*') if p.is_file() and not p.name.endswith('.partial')),
                           'coverage': 'Twelve nearby grid-cell samples; spatial marts use the Geofabrik extract footprint.',
                           'osm_scope': 'Contemporary mapped objects within 20 km; no historical infrastructure or capacity inference.'},
                  'sources': [
                      {'name': 'CHIRPS v3', 'status': 'ready', 'url': 'https://chc.ucsb.edu/data/chirps3', 'role': 'Monthly rainfall estimates (mm/month), UCSB CHC.'},
                      {'name': 'ERA5-Land via Open-Meteo', 'status': 'ready', 'url': 'https://open-meteo.com/en/docs/historical-weather-api', 'role': 'Hourly temperature and 0–7 cm volumetric soil moisture; CC BY 4.0; non-commercial free API.'},
                      {'name': 'OpenStreetMap / Geofabrik', 'status': 'ready', 'url': 'https://download.geofabrik.de/africa/morocco.html', 'role': 'Mapped services and roads; © OpenStreetMap contributors, ODbL.'}]}
    (DATA / 'metadata/source_snapshot.json').write_text(json.dumps(extraction, ensure_ascii=False), encoding='utf-8')
    (PROCESSED / 'footprint.geojson').write_text(json.dumps({'type': 'Feature', 'geometry': footprint(), 'properties': {}}), encoding='utf-8')
    print('Normalized provider records prepared for all seven Bronze tables', flush=True)


def local_silver():
    import duckdb
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("set threads=2")
    con.execute('set memory_limit = ?', [os.environ.get('OBSERVATORY_DUCKDB_MEMORY_LIMIT', '1GB')])
    con.execute('set preserve_insertion_order=false')
    con.execute("set timezone='UTC'")
    con.execute('create schema if not exists bronze')
    con.execute('create schema if not exists silver')
    con.execute('create schema if not exists quarantine')
    results = []
    for table in TABLES:
        glob = str(DATA / f'bronze/{table}/**/*.parquet').replace('\\', '/').replace("'", "''")
        con.execute(f"create or replace view bronze.{table} as select *, current_timestamp as _ingested_at, filename as _source_file, sha256(to_json(raw)) as _row_hash from read_parquet('{glob}', hive_partitioning=false, filename=true) raw")
        con.execute('create or replace temp view candidates as ' + cast_query(table, 'duckdb'))
        source_rows = con.execute('select count(*) from candidates').fetchone()[0]
        con.execute(f'create or replace table quarantine.{table} as select *, \'invalid_contract\' as reject_reason from candidates where not coalesce(({validity(table)}), false)')
        rejected = con.execute(f'select count(*) from quarantine.{table}').fetchone()[0]
        if not source_rows:
            con.close()
            raise ValueError(f'{table}: empty Bronze input; publishing blocked')
        if rejected:
            con.close()
            raise ValueError(f'{table}: {rejected} invalid records quarantined; publishing blocked')
        con.execute(f'create or replace table silver.{table} as ' + dedupe_query(table))
        accepted = con.execute(f'select count(*) from silver.{table}').fetchone()[0]
        results.append({'table': table, 'bronze_rows': source_rows, 'silver_rows': accepted, 'rejected': rejected, 'deduplicated': source_rows - accepted})
        print(f'Silver {table}: {accepted:,} accepted, {rejected} rejected', flush=True)
    con.close()
    (DATA / 'metadata/silver_verification.json').write_text(json.dumps({'engine': 'duckdb-local-verification', 'tables': results}, indent=2))


def run_dbt():
    from dbt.cli.main import dbtRunner
    os.environ['OBSERVATORY_WAREHOUSE'] = str(DB_PATH)
    os.environ['DBT_SEND_ANONYMOUS_USAGE_STATS'] = 'false'
    previous = Path.cwd()
    try:
        os.chdir(ROOT)
        result = dbtRunner().invoke(['build', '--project-dir', 'dbt', '--profiles-dir', 'dbt', '--target', 'local', '--warn-error'])
    finally:
        os.chdir(previous)
    if not result.success:
        raise RuntimeError('dbt build failed; Gold exports have not been updated')


def export_local():
    import duckdb
    results = json.loads((ROOT / 'dbt/target/run_results.json').read_text())
    validate_dbt_results(results, 'duckdb')
    con = duckdb.connect(str(DB_PATH), read_only=True)
    gold = DATA / 'gold'
    gold.mkdir(parents=True, exist_ok=True)
    for table in GOLD_TABLES:
        path = gold / f'{table}.csv'
        temporary = path.with_suffix('.csv.partial')
        quoted = str(temporary).replace('\\', '/').replace("'", "''")
        con.execute(f"copy (select * from gold.{table}) to '{quoted}' (header, delimiter ',')")
        temporary.replace(path)
    snapshot_path = gold / 'snapshot.json'
    snapshot = json.loads((DATA / 'metadata/source_snapshot.json').read_text(encoding='utf-8'))
    from datetime import datetime, timezone
    snapshot['meta']['decoded_at'] = snapshot['meta']['built_at']
    snapshot['meta']['built_at'] = datetime.now(timezone.utc).isoformat()
    fact = con.execute('select * from gold.fact_climate_monthly').fetchdf()
    snapshot['monthly'] = fact.to_dict('records')
    snapshot['meta']['gold_transformation_engine'] = 'dbt'
    snapshot['meta']['verification_target'] = 'duckdb-local; production target is Spark/Delta on MinIO'
    snapshot['meta']['baseline_period'] = [2015, 2019]
    snapshot['meta']['marts'] = GOLD_TABLES
    snapshot['meta']['dbt_invocation_id'] = results['metadata']['invocation_id']
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, allow_nan=False), encoding='utf-8')
    shutil.copyfile(DATA / 'processed/osm_services.geojson', gold / 'osm.geojson')
    shutil.copyfile(DATA / 'processed/footprint.geojson', gold / 'footprint.geojson')
    con.close()
    artifacts = ROOT / 'dbt/target'
    shutil.copyfile(artifacts / 'run_results.json', DATA / 'metadata/dbt_run_results.json')
    provenance = {'engine': 'dbt-duckdb', 'verification_scope': 'local; distributed stack not executed',
                  'dbt_invocation_id': results['metadata']['invocation_id'],
                  'source_manifests': ['metadata/climate_downloads.json', 'metadata/era5_downloads.json', 'metadata/osm_downloads.json'], 'tables': {}}
    osm_manifest = DATA / 'metadata/osm_downloads.json'
    if not osm_manifest.exists():
        record_osm_provenance()
    provenance['osm_files'] = json.loads(osm_manifest.read_text())['files']
    for table in GOLD_TABLES:
        with (gold / f'{table}.csv').open(encoding='utf-8', newline='') as stream:
            provenance['tables'][table] = sum(1 for _ in csv.DictReader(stream))
    (gold / 'provenance.json').write_text(json.dumps(provenance, indent=2), encoding='utf-8')
    print('Tested dbt Gold tables exported for Power BI and Streamlit', flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', choices=['prepare', 'local-silver', 'dbt', 'export', 'local-all'])
    parser.add_argument('--from-landing', action='store_true', help='Retrieve committed MinIO raw inputs and rebuild decoded records')
    args = parser.parse_args()
    if args.stage in ['prepare', 'local-all']:
        prepare_sources(from_landing=args.from_landing)
    if args.stage in ['local-silver', 'local-all']:
        local_silver()
    if args.stage in ['dbt', 'local-all']:
        run_dbt()
    if args.stage in ['export', 'local-all']:
        export_local()


if __name__ == '__main__':
    main()
