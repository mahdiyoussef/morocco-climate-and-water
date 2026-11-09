"""Build and validate the run-specific evidence required for distributed Gold."""
import argparse
import json
import os
import subprocess
import tempfile
from pathlib import Path

from observatory.run_scope import DATA, active_key, digest, receipt_path, schema, write_json
from observatory.storage import RAW_MANIFEST, STAGING_MANIFEST, client, read_manifest
from observatory.warehouse import validate_dbt_results

ARTIFACTS = ('manifest.json', 'run_results.json')


def validate_artifacts(manifest, results):
    validate_dbt_results(results, 'spark')
    invocation = results['metadata']['invocation_id']
    if not invocation or manifest['metadata'].get('invocation_id') != invocation:
        raise ValueError('dbt manifest and results must share an invocation ID')
    project = manifest['metadata']['project_name']
    required = {key: node['resource_type'] for key, node in manifest['nodes'].items()
                if node['package_name'] == project and node['resource_type'] in {'model', 'test', 'unit_test'}
                and node['config'].get('enabled', True)
                and node['config'].get('materialized') != 'ephemeral'}
    outcomes = {row['unique_id']: row['status'] for row in results['results']}
    if len(outcomes) != len(results['results']):
        raise ValueError('dbt results contain duplicate nodes')
    if not any(kind == 'test' for kind in required.values()) or any(
        outcomes.get(node) != ('success' if kind == 'model' else 'pass') for node, kind in required.items()
    ):
        raise ValueError('dbt results must include every enabled project model and quality test')


def inputs(s3):
    key = active_key()
    raw, raw_sha = read_manifest(s3, RAW_MANIFEST, key)
    normalized, normalized_sha = read_manifest(s3, STAGING_MANIFEST, key)
    if normalized.get('raw_manifest_sha256') != raw_sha:
        raise ValueError('Normalized Landing does not match the committed raw manifest')
    return normalized, {'raw': raw_sha, 'normalized': normalized_sha}


def verified_body(s3, bucket, entry):
    body = s3.get_object(Bucket=bucket, Key=entry['key'])['Body'].read()
    if len(body) != entry['bytes'] or digest(body) != entry['sha256']:
        raise ValueError(f"{bucket} object checksum mismatch: {entry['key']}")
    return body


def source_objects(s3, normalized):
    prefix = f'observatory/guardrails/{active_key()}/metadata/'
    names = ('source_snapshot.json', 'osm_downloads.json', 'osm_services.geojson', 'footprint.geojson')
    entries = {entry['path']: entry for entry in normalized['files'] if entry['key'].startswith(prefix)}
    sources = {}
    for name in names:
        entry = entries[name]
        if entry['key'] != prefix + name + '/' + entry['sha256']:
            raise ValueError('Source metadata does not match its run and fingerprint')
        sources[name] = verified_body(s3, 'landing', entry)
    return sources


def settings():
    key = active_key()
    if not key:
        raise ValueError('An Airflow run ID is required')
    return dict(os.environ, OBSERVATORY_STAGING_SCHEMA=schema('staging'),
                OBSERVATORY_SILVER_SCHEMA=schema('silver'), OBSERVATORY_GOLD_SCHEMA=schema('gold'),
                OBSERVATORY_GOLD_LOCATION=f's3a://gold/morocco/runs/{key}')


def build():
    path = receipt_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    s3 = client()
    normalized, hashes = inputs(s3)
    for stage in ('bronze', 'silver'):
        evidence = json.loads((path.parent / f'{stage}.json').read_text())
        if evidence['run_key'] != active_key() or evidence['manifest_sha256'] != hashes['normalized'] or evidence['status'] != 'passed':
            raise ValueError(f'{stage} receipt does not match this run and its normalized inputs')
    with tempfile.TemporaryDirectory(dir=path.parent) as directory:
        target = Path(directory)
        subprocess.run(['dbt', 'build', '--project-dir', 'dbt', '--profiles-dir', 'dbt',
                        '--target', 'spark', '--target-path', directory, '--warn-error'], env=settings(), check=True)
        artifacts = {name: (target / name).read_bytes() for name in ARTIFACTS}
    manifest, results = (json.loads(artifacts[name]) for name in ARTIFACTS)
    validate_artifacts(manifest, results)
    if inputs(s3)[1] != hashes:
        raise ValueError('Landing manifest changed during the dbt build')
    snapshot = json.loads(source_objects(s3, normalized)['source_snapshot.json'])
    meta = snapshot['meta']
    receipt = {'run_id': os.environ['OBSERVATORY_AIRFLOW_RUN_ID'], 'run_key': active_key(),
               'schema': schema('gold'), 'logical_date': os.environ['OBSERVATORY_AIRFLOW_LOGICAL_DATE'],
               'manifests': hashes, 'years': meta['years'], 'sites': meta['site_count'],
               'site_list_sha256': meta['site_list_sha256'],
               'dbt_invocation_id': results['metadata']['invocation_id'],
               'artifacts': {name: digest(body) for name, body in artifacts.items()}}
    for name, body in artifacts.items():
        (path.parent / name).write_bytes(body)
    write_json(path, receipt)
    write_json(DATA / 'metadata/dbt_candidate_run_results.json', results)


def load_build(s3):
    path = receipt_path()
    receipt = json.loads(path.read_text())
    if (receipt['run_id'] != os.environ['OBSERVATORY_AIRFLOW_RUN_ID'] or receipt['run_key'] != active_key()
            or receipt['schema'] != schema('gold')
            or receipt['logical_date'] != os.environ['OBSERVATORY_AIRFLOW_LOGICAL_DATE']):
        raise ValueError('Build receipt belongs to another Airflow run')
    normalized, hashes = inputs(s3)
    if receipt['manifests'] != hashes:
        raise ValueError('Build receipt does not match the committed Landing manifests')
    artifacts = {name: (path.parent / name).read_bytes() for name in ARTIFACTS}
    if {name: digest(body) for name, body in artifacts.items()} != receipt['artifacts']:
        raise ValueError('dbt artifact checksum mismatch')
    manifest, results = (json.loads(artifacts[name]) for name in ARTIFACTS)
    validate_artifacts(manifest, results)
    if results['metadata']['invocation_id'] != receipt['dbt_invocation_id']:
        raise ValueError('Build receipt has a different dbt invocation ID')
    return receipt, results, artifacts['run_results.json'], source_objects(s3, normalized)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['build', 'docs'])
    if parser.parse_args().command == 'build':
        try:
            build()
        except Exception:
            write_json(DATA / 'metadata/quality_dbt.json',
                       {'run_key': active_key(), 'stage': 'dbt', 'status': 'failed', 'rule': 'full_build_receipt',
                        'partition': 'gold', 'expected': 'complete current dbt build', 'actual': 'build or receipt failed'})
            raise
        write_json(DATA / 'metadata/quality_dbt.json',
                   {'run_key': active_key(), 'stage': 'dbt', 'status': 'passed', 'rule': 'full_build_receipt'})
    else:
        load_build(client())
        subprocess.run(['dbt', 'docs', 'generate', '--project-dir', 'dbt', '--profiles-dir', 'dbt',
                        '--target', 'spark'], env=settings(), check=True)


if __name__ == '__main__':
    main()
