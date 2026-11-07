"""Stage immutable provider inputs and normalized records in private MinIO buckets."""
import argparse
import hashlib
import json
import os
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data'
BUCKETS = ['landing', 'bronze', 'silver', 'gold', 'warehouse', 'exports', 'logs']


def client():
    return boto3.client('s3', endpoint_url=os.environ.get('MINIO_ENDPOINT', 'http://minio:9000'),
                        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
                        region_name=os.environ.get('AWS_REGION', 'us-east-1'),
                        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, retries={'max_attempts': 3}))


def put_file(s3, path, bucket, key):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            digest.update(block)
    sha = digest.hexdigest()
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
    results = []
    for path in sorted((DATA / 'raw').rglob('*')):
        if path.is_file() and not path.name.endswith(('.partial', '.landing-partial')):
            key = 'observatory/' + path.relative_to(DATA).as_posix()
            results.append(put_file(s3, path, 'landing', key))
    for name in ['climate_downloads.json', 'era5_downloads.json']:
        results.append(put_file(s3, DATA / 'metadata' / name, 'landing', 'observatory/metadata/' + name))
    s3.put_object(Bucket='landing', Key='observatory/metadata/raw_manifest.json',
                  Body=json.dumps({'files': results}).encode(), ContentType='application/json')
    print(f'Raw Landing committed: {len(results)} objects', flush=True)


def restore_raw():
    """Decode only raw inputs retrieved from the committed Landing manifest."""
    s3 = client()
    manifest = json.loads(s3.get_object(Bucket='landing', Key='observatory/metadata/raw_manifest.json')['Body'].read())
    for entry in manifest['files']:
        key = entry['key']
        relative = Path(key.removeprefix('observatory/'))
        if not key.startswith('observatory/') or relative.is_absolute() or '..' in relative.parts or relative.parts[0] not in {'raw', 'metadata'}:
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
    print(f'Retrieved and verified {len(manifest["files"])} Landing inputs for decoding', flush=True)


def sync():
    inputs = json.loads((DATA / 'metadata/bronze_inputs.json').read_text())
    s3 = client()
    ensure_buckets(s3)
    results = []
    for table, files in inputs['tables'].items():
        for entry in files:
            path = DATA / entry['path']
            relative = path.relative_to(DATA / 'bronze')
            results.append(put_file(s3, path, 'landing', 'observatory/normalized/' + relative.as_posix()))
    for name in ['climate_downloads.json', 'era5_downloads.json', 'osm_downloads.json', 'source_snapshot.json', 'bronze_inputs.json']:
        results.append(put_file(s3, DATA / 'metadata' / name, 'landing', 'observatory/metadata/' + name))
    s3.put_object(Bucket='landing', Key='observatory/metadata/staging_manifest.json',
                  Body=json.dumps({'files': results}).encode(), ContentType='application/json')
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
