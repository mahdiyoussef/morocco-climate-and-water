"""Run identity and atomic evidence shared by the pipeline stages."""
import hashlib
import json
import os
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data'
SOURCE_STAGES = ('acquire_and_verify', 'upload_verified_raw_landing',
                 'decode_provider_formats', 'upload_verified_minio_landing')


def run_key(run_id):
    return hashlib.sha256(run_id.encode()).hexdigest()[:32]


def active_key():
    run_id = os.environ.get('OBSERVATORY_AIRFLOW_RUN_ID')
    return run_key(run_id) if run_id else None


def schema(layer):
    key = active_key()
    return f'{layer}_{key}' if key else layer


def receipt_path():
    key = active_key()
    if not key:
        raise ValueError('An Airflow run ID is required for a guarded build')
    return DATA / 'runtime/guardrails' / key / 'build.json'


def digest(body):
    return hashlib.sha256(body).hexdigest()


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f'.{path.name}.{uuid.uuid4().hex}.tmp')
    try:
        with temporary.open('xb') as output:
            output.write(json.dumps(payload, sort_keys=True, allow_nan=False).encode())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
