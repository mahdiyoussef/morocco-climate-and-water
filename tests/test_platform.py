"""Check rejection, deduplication, upload checksums, and stack wiring."""
import hashlib
import io
import json
from pathlib import Path

import duckdb
import yaml
import pytest
from botocore.exceptions import ClientError
from observatory.contracts import dedupe_query, validity
from observatory.storage import put_file
from observatory.warehouse import GOLD_TABLES, validate_dbt_results


def test_raw_landing_restore_verifies_before_replacing(tmp_path, monkeypatch):
    from observatory import storage
    original = tmp_path / 'raw' / 'climate.nc'
    original.parent.mkdir()
    original.write_bytes(b'previous input')
    body = b'verified provider input'
    entry = {'key': 'observatory/raw/climate.nc', 'bytes': len(body),
             'sha256': hashlib.sha256(body).hexdigest()}

    class Landing:
        payload = body

        def get_object(self, **kwargs):
            return {'Body': io.BytesIO(json.dumps({'files': [entry]}).encode())}

        def download_file(self, bucket, key, path):
            Path(path).write_bytes(self.payload)

    landing = Landing()
    monkeypatch.setattr(storage, 'DATA', tmp_path)
    monkeypatch.setattr(storage, 'client', lambda: landing)
    landing.payload = b'corrupt'
    with pytest.raises(ValueError, match='checksum mismatch'):
        storage.restore_raw()
    assert original.read_bytes() == b'previous input'
    landing.payload = body
    storage.restore_raw()
    assert original.read_bytes() == body
    entry['key'] = 'observatory/raw/../../outside'
    with pytest.raises(ValueError, match='Invalid Landing input path'):
        storage.restore_raw()


def test_publication_requires_all_gold_models_and_passing_tests():
    results = {'args': {'target': 'local', 'which': 'build'}, 'results': [
        *[{'unique_id': 'model.morocco.' + table, 'status': 'success'} for table in GOLD_TABLES],
        {'unique_id': 'test.morocco.quality', 'status': 'pass'},
        {'unique_id': 'exposure.morocco.dashboard', 'status': 'no-op'},
    ]}
    validate_dbt_results(results, 'duckdb')
    with pytest.raises(ValueError):
        validate_dbt_results(results, 'spark')
    results['results'][-2]['status'] = 'skipped'
    with pytest.raises(ValueError):
        validate_dbt_results(results, 'duckdb')


def test_silver_rejects_invalid_and_deduplicates():
    con = duckdb.connect()
    con.execute('''create table candidates as select * from (values
        ('rabat',10.0,1,'a','h1'), ('rabat',10.0,2,'b','h2'),
        ('negative',-1.0,1,'a','h3'), ('missing',NULL,1,'a','h4'),
        ('nonfinite','NaN'::double,1,'a','h5')) t(site,road_km,_ingested_at,_source_file,_row_hash)''')
    assert con.execute('select count(*) from candidates where not coalesce((' + validity('osm_roads') + '),false)').fetchone()[0] == 3
    result = con.execute(dedupe_query('osm_roads')).fetchall()
    assert len(result) == 1 and result[0][2] == 2
    con.close()


class FakeS3:
    def __init__(self):
        self.files, self.uploads = {}, 0

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.files:
            raise ClientError({'Error': {'Code': '404'}}, 'HeadObject')
        return self.files[Bucket, Key]

    def upload_file(self, path, bucket, key, ExtraArgs):
        self.uploads += 1
        self.files[bucket, key] = {'ContentLength': Path(path).stat().st_size, 'Metadata': ExtraArgs['Metadata']}


def test_minio_upload_is_verified_and_reusable(tmp_path):
    source = tmp_path / 'source.bin'
    source.write_bytes(b'real content checksum')
    s3 = FakeS3()
    first = put_file(s3, source, 'landing', 'observatory/source.bin')
    second = put_file(s3, source, 'landing', 'observatory/source.bin')
    assert first['sha256'] == hashlib.sha256(source.read_bytes()).hexdigest()
    assert first['uploaded'] and not second['uploaded'] and s3.uploads == 1


def test_compose_dependency_and_mount_contracts():
    root = Path(__file__).resolve().parents[1]
    services = yaml.safe_load((root / 'docker-compose.yml').read_text())['services']
    for name, service in services.items():
        for dependency in service.get('depends_on', []):
            assert dependency in services, (name, dependency)
        for mount in service.get('volumes', []):
            path = mount.split(':', 1)[0]
            if path.startswith('./'):
                assert (root / path).exists(), (name, path)
    ports = [port.split(':')[1] for service in services.values() for port in service.get('ports', [])]
    assert len(ports) == len(set(ports))
    assert services['streamlit']['environment']['OBSERVATORY_DATA_BACKEND'] == 'postgres'
    assert services['airflow-scheduler']['environment']['AIRFLOW__CORE__DAGS_FOLDER'].endswith('/dags')
    assert len(json.loads((root / 'monitoring/grafana/dashboards/operations.json').read_text())['panels']) == 11
