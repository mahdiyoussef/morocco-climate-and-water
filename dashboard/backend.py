"""Read a committed, checksum-verified Gold snapshot from private MinIO objects."""
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path

ALLOWED = {'dim_location.csv', 'dim_month.csv', 'fact_climate_monthly.csv', 'fact_infrastructure.csv',
           'source_audit.csv', 'mart_monthly_anomalies.csv', 'mart_water_year.csv', 'mart_site_trends.csv',
           'mart_spatial_rainfall.csv', 'snapshot.json', 'osm.geojson', 'footprint.geojson', 'provenance.json',
           'dbt_run_results.json'}


def minio_snapshot():
    import boto3
    from botocore.client import Config
    s3 = boto3.client('s3', endpoint_url=os.environ['MINIO_ENDPOINT'],
                      aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                      aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                      region_name=os.environ.get('AWS_REGION', 'us-east-1'),
                      config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}))
    redis_client = None
    pointer = None
    if os.environ.get('REDIS_URL'):
        try:
            import redis
            redis_client = redis.Redis.from_url(os.environ['REDIS_URL'], socket_connect_timeout=1, socket_timeout=1)
            pointer = redis_client.get('morocco:committed_snapshot')
        except (OSError, redis.RedisError):
            redis_client = None
    if pointer is None:
        pointer = s3.get_object(Bucket='exports', Key='observatory/current.json')['Body'].read()
        if redis_client is not None:
            try:
                redis_client.setex('morocco:committed_snapshot', 15, pointer)
            except (OSError, redis.RedisError):
                pass
    pointer = json.loads(pointer)
    version = pointer['publication_id']
    if not re.fullmatch(r'[a-zA-Z0-9_-]{1,128}', version):
        raise ValueError('Invalid published snapshot identifier')
    root = Path(tempfile.gettempdir()) / 'observatory' / version
    if (root / '.complete').exists():
        return str(root)
    root.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=root.parent) as directory:
        stage = Path(directory)
        (stage / 'gold').mkdir()
        (stage / 'metadata').mkdir()
        names = set()
        for entry in pointer['files']:
            name = entry['key'].rsplit('/', 1)[-1]
            if name not in ALLOWED or entry['key'] != f'observatory/runs/{version}/{name}' or name in names:
                raise ValueError('Invalid object in committed Gold manifest')
            names.add(name)
            body = s3.get_object(Bucket='exports', Key=entry['key'])['Body'].read()
            if len(body) != entry['bytes'] or hashlib.sha256(body).hexdigest() != entry['sha256']:
                raise ValueError('Gold object checksum mismatch: ' + name)
            folder = 'metadata' if name == 'dbt_run_results.json' else 'gold'
            (stage / folder / name).write_bytes(body)
        required = {'dim_location.csv', 'dim_month.csv', 'fact_climate_monthly.csv', 'fact_infrastructure.csv', 'snapshot.json'}
        if not required <= names:
            raise ValueError('Published snapshot is incomplete')
        (stage / '.complete').write_text(version)
        try:
            stage.rename(root)
        except FileExistsError:
            if not (root / '.complete').exists():
                raise
    return str(root)
