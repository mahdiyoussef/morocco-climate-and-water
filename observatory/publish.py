"""Publish tested dbt Gold marts to PostgreSQL and a versioned MinIO snapshot."""
import argparse
import csv
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
from observatory.serving import commit_tables, current_promotion, publication_session, publish_tables
from observatory.run_scope import active_key, schema, write_json
from observatory import guardrails, promotion


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


def read_spark_tables(namespace):
    from pyhive import hive
    connection = hive.Connection(host=os.environ.get('SPARK_THRIFT_HOST', 'spark-thrift-server'), port=10000, auth='NOSASL')
    staged = {}
    try:
        for table in GOLD_TABLES:
            cursor = connection.cursor()
            cursor.execute(f'describe {namespace}.{table}')
            description = [(r[0], r[1]) for r in cursor.fetchall() if r[0] and not r[0].startswith('#') and r[1]]
            cursor.execute(f'select * from {namespace}.{table}')
            rows = cursor.fetchall()
            columns = [c[0].split('.')[-1] for c in cursor.description]
            types = dict(description)
            staged[table] = {'columns': columns, 'types': [pg_type(types[c]) for c in columns], 'rows': rows}
    finally:
        connection.close()
    return staged


def write_csvs(directory, staged):
    for table, value in staged.items():
        with (directory / f'{table}.csv').open('w', newline='', encoding='utf-8') as output:
            writer = csv.writer(output)
            writer.writerow(value['columns'])
            writer.writerows(value['rows'])


def publish_guarded(conn, s3):
    key = active_key()
    previous = current_promotion(conn)
    if previous:
        if not previous['complete']:
            promotion.finish(conn, s3, DATA, previous)
        if previous['publication_id'] == key:
            return
    build, results, result_body, sources = guardrails.load_build(s3)
    if previous and datetime.fromisoformat(build['logical_date']) <= datetime.fromisoformat(previous['logical_date']):
        raise ValueError('Airflow run is older than the committed publication')
    source = json.loads(sources['source_snapshot.json'])
    if (source['meta']['site_list_sha256'] != build['site_list_sha256']
            or source['meta']['years'] != build['years'] or source['meta']['site_count'] != build['sites']):
        raise ValueError('Source scope does not match the build receipt')

    previous_snapshot = None
    if conn.execute("select to_regclass('gold.dashboard_metadata')").fetchone()[0]:
        row = conn.execute("select payload from gold.dashboard_metadata where name = 'snapshot'").fetchone()
        previous_snapshot = row[0] if row else None
    staged = read_spark_tables(schema('gold'))
    report = promotion.inspect_gold(staged, source, previous_snapshot, key)
    write_json(DATA / 'metadata/quality_publish.json', report)
    if report['status'] == 'failed':
        raise ValueError('Gold site, month or hour coverage does not match the run receipt')

    stamp = promotion.timestamp()
    source['meta'].update(decoded_at=source['meta']['built_at'], built_at=stamp, publication_id=key,
                          gold_transformation_engine='dbt-spark', medallion='MinIO / Spark Delta Bronze+Silver / dbt Gold',
                          dbt_invocation_id=build['dbt_invocation_id'])
    fact = staged['fact_climate_monthly']
    source['monthly'] = [dict(zip(fact['columns'], row, strict=True)) for row in fact['rows']]
    counts = {table: len(data['rows']) for table, data in staged.items()}
    provenance = {'publication_id': key, 'dbt_invocation_id': build['dbt_invocation_id'],
                  'tables': counts, 'run': build, 'quality': report,
                  'osm_files': json.loads(sources['osm_downloads.json'])['files']}
    with tempfile.TemporaryDirectory(prefix='morocco-guarded-publish-') as directory:
        stage = Path(directory)
        write_csvs(stage, staged)
        (stage / 'snapshot.json').write_text(json.dumps(source, ensure_ascii=False, default=str), encoding='utf-8')
        write_json(stage / 'provenance.json', provenance)
        (stage / 'osm.geojson').write_bytes(sources['osm_services.geojson'])
        (stage / 'footprint.geojson').write_bytes(sources['footprint.geojson'])
        (stage / 'dbt_run_results.json').write_bytes(result_body)
        assets = {name: json.loads((stage / name).read_text()) for name in ('osm.geojson', 'footprint.geojson', 'provenance.json')}
        files = [put_file(s3, file, 'exports', f'observatory/runs/{key}/{file.name}') for file in sorted(stage.iterdir())]
        receipt = {'publication_id': key, 'logical_date': build['logical_date'],
                   'published_at': stamp, 'tables': counts, 'files': files, 'complete': False}
        promotion.stage_download(s3, receipt, stage)
        write_json(DATA / 'metadata/quality_publish.json', {**report, 'status': 'promoting'})
        commit_tables(conn, staged, source, assets, results, key, receipt)
        promotion.finish(conn, s3, DATA, receipt)


def repair():
    with publication_session() as conn:
        receipt = current_promotion(conn)
        if not receipt:
            raise ValueError('No guarded PostgreSQL publication is available to repair')
        promotion.finish(conn, client(), DATA, receipt)
        print(f"Publication {receipt['publication_id']} is complete", flush=True)


def main():
    if active_key():
        try:
            with publication_session() as conn:
                publish_guarded(conn, client())
        except Exception:
            path = DATA / 'metadata/quality_publish.json'
            report = json.loads(path.read_text()) if path.exists() else {}
            if report.get('run_key') != active_key() or report.get('status') != 'failed':
                report.update(run_key=active_key(), stage='publish', status='failed', rule='publication_promotion',
                              partition='gold', expected='matching committed publication IDs', actual='promotion interrupted')
                write_json(path, report)
            raise
        return

    invocation = os.environ.get('OBSERVATORY_RUN_ID') or uuid.uuid4().hex
    results_path = Path(os.environ.get('DBT_RUN_RESULTS', '/workspace/data/metadata/dbt_run_results.json'))
    results = json.loads(results_path.read_text())
    validate_dbt_results(results, 'spark')
    staged = read_spark_tables('gold')
    s3 = client()
    prefix = f'observatory/runs/{invocation}'
    exported = []
    with tempfile.TemporaryDirectory(prefix='morocco-publish-') as temp:
        temp = Path(temp)
        write_csvs(temp, staged)
        for table in staged:
            target = temp / f'{table}.csv'
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
    parser = argparse.ArgumentParser()
    parser.add_argument('command', nargs='?', default='publish', choices=['publish', 'repair', 'legacy-publish'])
    command = parser.parse_args().command
    if command == 'publish' and not active_key():
        parser.error('An Airflow run ID is required. Use legacy-publish for explicitly unscoped Spark data.')
    {'publish': main, 'repair': repair, 'legacy-publish': main}[command]()
