"""Publish tested dbt Gold marts to PostgreSQL and a versioned MinIO snapshot."""
import csv
import hashlib
import json
import os
import shutil
import socket
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


from observatory.storage import client, put_file, DATA
from observatory.warehouse import GOLD_TABLES, validate_dbt_results
from observatory.serving import publish_tables


def pg_type(spark_type):
    kind = spark_type.lower()
    if kind in ['int', 'bigint', 'smallint', 'tinyint', 'long']:
        return 'BIGINT'
    if kind in ['double', 'float'] or kind.startswith('decimal'):
        return 'DOUBLE PRECISION'
    if kind == 'boolean':
        return 'BOOLEAN'
    if kind == 'date':
        return 'DATE'
    if kind.startswith('timestamp'):
        return 'TIMESTAMP'
    return 'TEXT'


def main():
    from pyhive import hive
    invocation = os.environ.get('OBSERVATORY_RUN_ID') or uuid.uuid4().hex
    # Fail closed unless dbt build (models AND tests) succeeded for this publication.
    results_path = Path(os.environ.get('DBT_RUN_RESULTS', '/workspace/data/metadata/dbt_run_results.json'))
    results = json.loads(results_path.read_text())
    validate_dbt_results(results, 'spark')
    connection = hive.Connection(host=os.environ.get('SPARK_THRIFT_HOST', 'spark-thrift-server'), port=10000, auth='NOSASL')
    staged = {}
    try:
        for table in GOLD_TABLES:
            cursor = connection.cursor()
            cursor.execute(f'describe gold.{table}')
            description = [(r[0], r[1]) for r in cursor.fetchall() if r[0] and not r[0].startswith('#') and r[1]]
            cursor.execute(f'select * from gold.{table}')
            rows = cursor.fetchall()
            columns = [c[0].split('.')[-1] for c in cursor.description]
            types = dict(description)
            staged[table] = {'columns': columns, 'types': [pg_type(types[c]) for c in columns], 'rows': rows}
    finally:
        connection.close()
    s3 = client()
    prefix = f'observatory/runs/{invocation}'
    exported = []
    with tempfile.TemporaryDirectory(prefix='morocco-publish-') as temp:
        temp = Path(temp)
        for table, value in staged.items():
            target = temp / f'{table}.csv'
            with target.open('w', newline='', encoding='utf-8') as output:
                writer = csv.writer(output)
                writer.writerow(value['columns'])
                writer.writerows(value['rows'])
            exported.append(put_file(s3, target, 'exports', prefix + '/' + target.name))
        snapshot = json.loads((DATA / 'metadata/source_snapshot.json').read_text(encoding='utf-8'))
        snapshot['meta']['decoded_at'] = snapshot['meta']['built_at']
        snapshot['meta']['built_at'] = datetime.now(timezone.utc).isoformat()
        snapshot['meta'].update(gold_transformation_engine='dbt-spark', medallion='MinIO / Spark Delta Bronze+Silver / dbt Gold',
                                dbt_invocation_id=results['metadata']['invocation_id'], publication_id=invocation)
        snapshot['monthly'] = [dict(zip(staged['fact_climate_monthly']['columns'], row)) for row in staged['fact_climate_monthly']['rows']]
        (temp / 'snapshot.json').write_text(json.dumps(snapshot, ensure_ascii=False, default=str), encoding='utf-8')
        shutil.copyfile(DATA / 'processed/osm_services.geojson', temp / 'osm.geojson')
        shutil.copyfile(DATA / 'processed/footprint.geojson', temp / 'footprint.geojson')
        provenance = {'publication_id': invocation, 'dbt_invocation_id': results['metadata']['invocation_id'],
                      'source_manifests': ['s3://landing/observatory/metadata/climate_downloads.json', 's3://landing/observatory/metadata/era5_downloads.json', 's3://landing/observatory/metadata/osm_downloads.json'],
                      'osm_files': json.loads((DATA / 'metadata/osm_downloads.json').read_text())['files'],
                      'tables': {t: len(v['rows']) for t, v in staged.items()}}
        (temp / 'provenance.json').write_text(json.dumps(provenance, indent=2), encoding='utf-8')
        for name in ['snapshot.json', 'osm.geojson', 'footprint.geojson', 'provenance.json']:
            exported.append(put_file(s3, temp / name, 'exports', prefix + '/' + name))
        shutil.copyfile(results_path, temp / 'dbt_run_results.json')
        exported.append(put_file(s3, temp / 'dbt_run_results.json', 'exports', prefix + '/dbt_run_results.json'))
        assets = {name: json.loads((temp / name).read_text(encoding='utf-8')) for name in ['osm.geojson', 'footprint.geojson', 'provenance.json']}
        publish_tables(staged, snapshot, assets, results, invocation)
        # Commit marker is last: readers never see an incomplete versioned snapshot.
        pointer = {'publication_id': invocation, 'published_at': datetime.now(timezone.utc).isoformat(), 'files': exported}
        s3.put_object(Bucket='exports', Key='observatory/current.json', Body=json.dumps(pointer).encode(), ContentType='application/json')
        (DATA / 'gold').mkdir(parents=True, exist_ok=True)
        for path in temp.iterdir():
            destination = DATA / 'gold' / path.name
            shutil.copyfile(path, destination.with_suffix(destination.suffix + '.partial'))
            destination.with_suffix(destination.suffix + '.partial').replace(destination)
    state = {'last_success_timestamp_seconds': time.time(), 'gold_rows': len(staged['fact_climate_monthly']['rows']),
             'dbt_tests_failed': 0, 'publication_id': invocation}
    (DATA / 'metadata/publication.json').write_text(json.dumps(state), encoding='utf-8')
    evidence_path = DATA / 'metadata/dbt_run_results.json'
    if results_path.resolve() != evidence_path.resolve():
        shutil.copyfile(results_path, evidence_path)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.sendto(f'morocco.pipeline.last_success_timestamp_seconds:{time.time()}|g'.encode(), ('statsd-exporter', 8125))
    except OSError:
        pass
    print(json.dumps({'published': invocation, 'tables': {t: len(v['rows']) for t, v in staged.items()}}), flush=True)


if __name__ == '__main__':
    main()
