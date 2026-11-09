"""Stage immutable provider inputs and normalized records in private MinIO buckets."""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path, PurePosixPath

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from observatory.contracts import TABLES
from observatory.run_scope import active_key, digest, receipt_path, write_json

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data'
BUCKETS = ['landing', 'bronze', 'silver', 'gold', 'warehouse', 'exports', 'logs']
NORMALIZED_PREFIX = 'observatory/normalized/'
STAGING_MANIFEST = 'observatory/metadata/staging_manifest.json'
RAW_MANIFEST = 'observatory/metadata/raw_manifest.json'


def manifest_key(name, key):
    return f'observatory/guardrails/{key}/{PurePosixPath(name).name}' if key else name


def read_manifest(s3, name, key):
    body = s3.get_object(Bucket='landing', Key=manifest_key(name, key))['Body'].read()
    manifest = json.loads(body)
    if key and manifest.get('run_key') != key:
        raise ValueError('Landing manifest belongs to a different run')
    return manifest, digest(body)


def commit_manifest(s3, name, key, files):
    manifest = {'files': files}
    if key:
        manifest = {'run_key': key, 'files': [{k: v for k, v in entry.items() if k != 'uploaded'} for entry in files]}
        if name == STAGING_MANIFEST:
            _, raw_sha = read_manifest(s3, RAW_MANIFEST, key)
            restored = json.loads((receipt_path().parent / 'raw_restored.json').read_text())
            if raw_sha != restored['raw_manifest_sha256']:
                raise ValueError('Raw Landing changed after normalization inputs were restored')
            manifest['raw_manifest_sha256'] = raw_sha
    s3.put_object(Bucket='landing', Key=manifest_key(name, key),
                  Body=json.dumps(manifest, sort_keys=True).encode(), ContentType='application/json')


def client():
    return boto3.client('s3', endpoint_url=os.environ.get('MINIO_ENDPOINT', 'http://minio:9000'),
                        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
                        region_name=os.environ.get('AWS_REGION', 'us-east-1'),
                        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, retries={'max_attempts': 3}))


def put_file(s3, path, bucket, key, *, expected=None, content_addressed=False):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            digest.update(block)
    sha = digest.hexdigest()
    size = path.stat().st_size
    if content_addressed:
        key = f"{key}/{sha}"
    if expected is not None and (size != expected['bytes'] or sha != expected['sha256']):
        raise ValueError(f'Normalized input does not match its manifest: {path.name}')
    try:
        existing = s3.head_object(Bucket=bucket, Key=key)
        if existing['ContentLength'] == path.stat().st_size and existing.get('Metadata', {}).get('sha256') == sha:
            return {'key': key, 'sha256': sha, 'bytes': path.stat().st_size, 'uploaded': False}
    except ClientError as exc:
        if exc.response['Error']['Code'] not in ['404', 'NoSuchKey', 'NotFound']:
            raise
    s3.upload_file(str(path), bucket, key, ExtraArgs={'Metadata': {'sha256': sha}})
    remote = s3.head_object(Bucket=bucket, Key=key)
    if remote['ContentLength'] != path.stat().st_size or remote.get('Metadata', {}).get('sha256') != sha:
        raise ValueError('MinIO upload verification failed: ' + key)
    return {'key': key, 'sha256': sha, 'bytes': path.stat().st_size, 'uploaded': True}


def ensure_buckets(s3):
    available = {bucket['Name'] for bucket in s3.list_buckets()['Buckets']}
    for bucket in BUCKETS:
        if bucket not in available:
            s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(Bucket='landing', VersioningConfiguration={'Status': 'Enabled'})


def initialize_work_cache():
    root = os.environ.get('OBSERVATORY_CACHE_ROOT')
    if root:
        uid = int(os.environ.get('OBSERVATORY_CACHE_UID', '50000'))
        for name in ['raw', 'processed', 'bronze', 'ivy']:
            path = Path(root) / name
            path.mkdir(parents=True, exist_ok=True)
            os.chown(path, uid, 0)
            path.chmod(0o2775)


def sync_raw():
    manifest = json.loads((DATA / 'metadata/climate_downloads.json').read_text())
    if not manifest.get('minimum_size_met') or not manifest.get('final_verification_utc'):
        raise ValueError('Verified climate input volume above 1 GiB is required')
    s3 = client()
    ensure_buckets(s3)
    run = active_key()
    prefix = f'observatory/guardrails/{run}/input/' if run else 'observatory/'
    results = []
    for path in sorted((DATA / 'raw').rglob('*')):
        if path.is_file() and not path.name.endswith(('.partial', '.landing-partial')):
            logical = path.relative_to(DATA).as_posix()
            entry = put_file(s3, path, 'landing', prefix + logical, content_addressed=bool(run))
            results.append({**entry, 'path': logical} if run else entry)
    for name in ['climate_downloads.json', 'era5_downloads.json']:
        logical = f'metadata/{name}'
        entry = put_file(s3, DATA / logical, 'landing', prefix + logical, content_addressed=bool(run))
        results.append({**entry, 'path': logical} if run else entry)
    commit_manifest(s3, RAW_MANIFEST, run, results)
    print(f'Raw Landing committed: {len(results)} objects', flush=True)


def restore_raw():
    """Decode only raw inputs retrieved from the committed Landing manifest."""
    s3 = client()
    run = active_key()
    manifest, raw_sha = read_manifest(s3, RAW_MANIFEST, run)
    prefix = f'observatory/guardrails/{run}/input/' if run else 'observatory/'
    for entry in manifest['files']:
        key = entry['key']
        logical = entry['path'] if run else key.removeprefix(prefix)
        if run and key != prefix + logical + '/' + entry['sha256']:
            raise ValueError('Raw Landing object does not match its run and fingerprint')
        relative = Path(logical)
        if not key.startswith(prefix) or relative.is_absolute() or '..' in relative.parts or len(relative.parts) < 2 or relative.parts[0] not in {'raw', 'metadata'}:
            raise ValueError('Invalid Landing input path')
        target = DATA / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(target.suffix + '.landing-partial')
        try:
            s3.download_file('landing', key, str(temporary))
            digest = hashlib.sha256()
            with temporary.open('rb') as stream:
                for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
                    digest.update(block)
            if temporary.stat().st_size != entry['bytes'] or digest.hexdigest() != entry['sha256']:
                raise ValueError('Landing input checksum mismatch: ' + key)
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)
    if run:
        write_json(receipt_path().parent / 'raw_restored.json', {'raw_manifest_sha256': raw_sha})
    print(f'Retrieved and verified {len(manifest["files"])} Landing inputs for decoding', flush=True)


def validate_normalized_entry(name, entry, prefix):
    path = PurePosixPath(name)
    if (not name.startswith(prefix) or path.is_absolute() or path.as_posix() != name
            or '..' in path.parts or path.suffix != '.parquet'
            or any(not re.fullmatch(r'[A-Za-z0-9_.=-]+', part) for part in path.parts)):
        raise ValueError(f'Invalid normalized Parquet path: {name}')
    if (type(entry.get('bytes')) is not int or entry['bytes'] <= 0
            or not re.fullmatch(r'[0-9a-f]{64}', str(entry.get('sha256', '')))):
        raise ValueError(f'Invalid normalized Parquet fingerprint: {name}')


def staged_inputs(s3):
    """Resolve only the Parquet objects listed in the committed Landing manifest."""
    manifest, _ = read_manifest(s3, STAGING_MANIFEST, active_key())
    tables = {table: [] for table in TABLES}
    seen = set()
    for entry in manifest['files']:
        key = entry['key']
        if not key.startswith(NORMALIZED_PREFIX):
            continue
        table, _, filename = key[len(NORMALIZED_PREFIX):].partition('/')
        if table not in tables or not filename or key in seen:
            raise ValueError(f'Unexpected or duplicate normalized Landing input: {key}')
        validate_normalized_entry(key, entry, f'{NORMALIZED_PREFIX}{table}/')
        seen.add(key)
        tables[table].append(f's3a://landing/{key}')
    missing = [table for table, paths in tables.items() if not paths]
    if missing:
        raise ValueError(f'Landing manifest is missing required tables: {", ".join(missing)}')
    return tables


def sync():
    inputs = json.loads((DATA / 'metadata/bronze_inputs.json').read_text())
    if set(inputs['tables']) != set(TABLES) or any(not files for files in inputs['tables'].values()):
        raise ValueError('Normalized manifest must list inputs for every Bronze table')
    for table, files in inputs['tables'].items():
        names = [entry['path'] for entry in files]
        if len(names) != len(set(names)):
            raise ValueError(f'Duplicate normalized inputs for {table}')
        for entry in files:
            validate_normalized_entry(entry['path'], entry, f'bronze/{table}/')
    s3 = client()
    ensure_buckets(s3)
    results = []
    for table, files in inputs['tables'].items():
        for entry in files:
            path = DATA / entry['path']
            relative = path.relative_to(DATA / 'bronze')
            # A failed upload must leave objects referenced by the previous manifest intact.
            relative = relative.with_name(f"{relative.stem}-{entry['sha256']}.parquet")
            results.append(put_file(s3, path, 'landing', NORMALIZED_PREFIX + relative.as_posix(), expected=entry))
    run = active_key()
    prefix = f'observatory/guardrails/{run}/metadata/' if run else 'observatory/metadata/'
    for name in ['climate_downloads.json', 'era5_downloads.json', 'osm_downloads.json', 'source_snapshot.json', 'bronze_inputs.json']:
        entry = put_file(s3, DATA / 'metadata' / name, 'landing', prefix + name, content_addressed=bool(run))
        results.append({**entry, 'path': name} if run else entry)
    if run:
        for name in ('osm_services.geojson', 'footprint.geojson'):
            entry = put_file(s3, DATA / 'processed' / name, 'landing', prefix + name, content_addressed=True)
            results.append({**entry, 'path': name})
    commit_manifest(s3, STAGING_MANIFEST, run, results)
    print(f'Landing verified: {len(results)} inputs; {sum(r["bytes"] for r in results):,} bytes', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['sync', 'sync-raw', 'restore-raw', 'init'])
    command = parser.parse_args().command
    if command == 'init':
        ensure_buckets(client())
        initialize_work_cache()
    else:
        {'sync': sync, 'sync-raw': sync_raw, 'restore-raw': restore_raw}[command]()
