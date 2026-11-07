"""A bad commit marker or altered object must never become a dashboard snapshot."""
import hashlib
import io
import json

import pytest

from dashboard.backend import minio_snapshot


class SnapshotS3:
    def __init__(self, corrupt=False, invalid_path=False):
        self.objects = {}
        names = ['dim_location.csv', 'dim_month.csv', 'fact_climate_monthly.csv', 'fact_infrastructure.csv', 'snapshot.json']
        files = []
        for name in names:
            body = b'{"meta":{}}' if name.endswith('.json') else b'column\nvalue\n'
            key = 'observatory/runs/test_snapshot/' + name
            files.append({'key': key, 'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()})
            self.objects[key] = body
        if corrupt:
            self.objects[files[0]['key']] = b'altered content'
        if invalid_path:
            files[0]['key'] = '../dim_location.csv'
        self.objects['observatory/current.json'] = json.dumps({'publication_id': 'test_snapshot', 'files': files}).encode()

    def get_object(self, Bucket, Key):
        return {'Body': io.BytesIO(self.objects[Key])}


@pytest.mark.parametrize('corrupt,invalid_path', [(True, False), (False, True)])
def test_snapshot_rejects_corruption_and_path_escape(tmp_path, monkeypatch, corrupt, invalid_path):
    import boto3
    monkeypatch.setenv('MINIO_ENDPOINT', 'http://minio:9000')
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'test-key')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'test-secret')
    monkeypatch.delenv('REDIS_URL', raising=False)
    monkeypatch.setattr('tempfile.gettempdir', lambda: str(tmp_path))
    monkeypatch.setattr(boto3, 'client', lambda *args, **kwargs: SnapshotS3(corrupt, invalid_path))
    with pytest.raises(ValueError):
        minio_snapshot()
    assert not (tmp_path / 'observatory/test_snapshot/.complete').exists()
