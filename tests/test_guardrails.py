"""Fail closed on stale receipts and replay only checksum-verified exports."""
import io
import json
from calendar import monthrange
from unittest.mock import Mock

import pytest

from observatory import guardrails, promotion, run_scope
from observatory.warehouse import GOLD_TABLES


def build_artifacts():
    project = 'morocco_observatory'
    models = [f'model.{project}.{table}' for table in GOLD_TABLES]
    tests = [f'test.{project}.{name}' for name in ('coverage', 'quality')]
    nodes = {name: {'resource_type': name.split('.')[0], 'package_name': project, 'config': {}}
             for name in models + tests}
    manifest = {'metadata': {'project_name': project, 'invocation_id': 'build'}, 'nodes': nodes}
    results = {'metadata': {'invocation_id': 'build'}, 'args': {'target': 'spark', 'which': 'build'},
               'results': [{'unique_id': name, 'status': 'success' if name in models else 'pass'} for name in nodes]}
    return manifest, results


@pytest.mark.parametrize('fault', [None, 'stale', 'missing', 'skipped', 'duplicate'])
def test_every_enabled_dbt_check_must_run(fault):
    manifest, results = build_artifacts()
    if fault == 'stale':
        results['metadata']['invocation_id'] = 'older'
    elif fault == 'missing':
        results['results'].pop()
    elif fault == 'skipped':
        results['results'][-1]['status'] = 'skipped'
    elif fault == 'duplicate':
        results['results'].append(results['results'][-1])
    if fault:
        with pytest.raises(ValueError):
            guardrails.validate_artifacts(manifest, results)
    else:
        guardrails.validate_artifacts(manifest, results)


@pytest.mark.parametrize('fault', ['stale_run', 'changed_landing', 'changed_artifact'])
def test_build_receipt_binds_current_inputs_and_artifacts(tmp_path, monkeypatch, fault):
    monkeypatch.setattr(run_scope, 'DATA', tmp_path)
    monkeypatch.setenv('OBSERVATORY_AIRFLOW_RUN_ID', 'scheduled__2024')
    monkeypatch.setenv('OBSERVATORY_AIRFLOW_LOGICAL_DATE', '2024-01-01T00:00:00+00:00')
    path = run_scope.receipt_path()
    manifest, results = build_artifacts()
    artifacts = {name: json.dumps(body).encode() for name, body in zip(guardrails.ARTIFACTS, (manifest, results))}
    hashes = {'raw': 'raw', 'normalized': 'normalized'}
    receipt = {'run_id': 'scheduled__2023' if fault == 'stale_run' else 'scheduled__2024',
               'run_key': run_scope.active_key(), 'schema': run_scope.schema('gold'),
               'logical_date': '2024-01-01T00:00:00+00:00', 'dbt_invocation_id': 'build',
               'manifests': hashes, 'artifacts': {name: run_scope.digest(body) for name, body in artifacts.items()}}
    run_scope.write_json(path, receipt)
    for name, body in artifacts.items():
        (path.parent / name).write_bytes(body)
    if fault == 'changed_artifact':
        (path.parent / 'manifest.json').write_text('{}')
    current = {**hashes, 'normalized': 'other'} if fault == 'changed_landing' else hashes
    monkeypatch.setattr(guardrails, 'inputs', lambda _: ({}, current))
    with pytest.raises(ValueError, match='another Airflow run|Landing manifests|artifact checksum'):
        guardrails.load_build(Mock())


def test_gold_coverage_blocks_gaps_and_reports_seasonal_drift():
    source = {'meta': {'site_ids': ['rabat'], 'site_count': 1, 'years': [2024]}}
    columns = ['site', 'year', 'month', 'hours', *promotion.METRICS]
    rows = [('rabat', 2024, month, 24 * monthrange(2024, month)[1], 20, 20, .3) for month in range(1, 13)]
    previous = {'monthly': [dict(zip(columns, (*row[:5], 10, row[6]))) for row in rows]}
    fact = {'columns': columns, 'rows': rows}
    report = promotion.inspect_gold({'fact_climate_monthly': fact}, source, previous, 'run')
    assert report['status'] == 'passed'
    assert len(report['warnings']) == 12
    fact['rows'].pop()
    report = promotion.inspect_gold({'fact_climate_monthly': fact}, source, previous, 'run')
    assert report['status'] == 'failed' and report['missing_keys'] == 1


def exports(version):
    tables = {table: 1 for table in GOLD_TABLES}
    payloads = {name: b'fixture' for name in promotion.ASSETS | {f'{table}.csv' for table in GOLD_TABLES}}
    payloads['snapshot.json'] = json.dumps({'meta': {'publication_id': version}}).encode()
    payloads['provenance.json'] = json.dumps({'publication_id': version, 'tables': tables}).encode()
    payloads['dbt_run_results.json'] = b'{}'
    objects = {f'observatory/runs/{version}/{name}': body for name, body in payloads.items()}
    receipt = {'publication_id': version, 'tables': tables, 'published_at': '2024-01-01T00:00:00+00:00',
               'logical_date': '2024-01-01T00:00:00+00:00', 'complete': False,
               'files': [{'key': key, 'bytes': len(body), 'sha256': run_scope.digest(body)}
                         for key, body in objects.items()]}
    return objects, receipt


@pytest.mark.parametrize('fault', ['checksum', 'pointer', 'local'])
def test_repair_replays_after_interrupted_promotion(tmp_path, monkeypatch, fault):
    objects, receipt = exports('sample-run')
    remote = Mock()
    remote.get_object.side_effect = lambda *, Bucket, Key: {'Body': io.BytesIO(objects[Key])}
    remote.put_object.side_effect = lambda **call: objects.update({call['Key']: call['Body']})
    conn = Mock()
    key = receipt['files'][0]['key']
    original = objects[key]
    from pathlib import Path
    real_replace = Path.replace
    if fault == 'checksum':
        objects[key] = b'broken'
    elif fault == 'pointer':
        remote.put_object.side_effect = OSError('pointer unavailable')
    else:
        def fail_local(self, target):
            if target.name.endswith('.csv'):
                raise OSError('local unavailable')
            return real_replace(self, target)
        monkeypatch.setattr(Path, 'replace', fail_local)
    with pytest.raises((OSError, ValueError)):
        promotion.finish(conn, remote, tmp_path, receipt)
    conn.execute.assert_not_called()
    assert promotion.CURRENT not in objects
    assert (tmp_path / 'gold/.promotion-pending').exists() == (fault != 'checksum')

    monkeypatch.setattr(Path, 'replace', real_replace)
    objects[key] = original
    remote.put_object.side_effect = lambda **call: objects.update({call['Key']: call['Body']})
    promotion.finish(conn, remote, tmp_path, receipt)
    assert json.loads(objects[promotion.CURRENT])['publication_id'] == 'sample-run'
    assert json.loads((tmp_path / 'metadata/publication.json').read_text())['publication_id'] == 'sample-run'
    assert not (tmp_path / 'gold/.promotion-pending').exists()
    conn.execute.assert_called_once()


def test_guarded_raw_landing_replay_keeps_manifest_digest_stable(tmp_path, monkeypatch):
    from pathlib import Path
    from observatory import storage
    from test_etl_quality import LandingStore

    store = LandingStore()
    monkeypatch.setattr(storage, 'DATA', tmp_path)
    monkeypatch.setattr(run_scope, 'DATA', tmp_path)
    monkeypatch.setattr(storage, 'client', lambda: store)
    monkeypatch.setattr(storage, 'ensure_buckets', lambda _: None)
    monkeypatch.setenv('OBSERVATORY_AIRFLOW_RUN_ID', 'scheduled__2024')
    raw = tmp_path / 'raw'
    raw.mkdir()
    sample = raw / 'source.nc'
    sample.write_bytes(b'first source')
    metadata = tmp_path / 'metadata'
    metadata.mkdir()
    (metadata / 'climate_downloads.json').write_text('{"minimum_size_met":true,"final_verification_utc":"verified"}')
    (metadata / 'era5_downloads.json').write_text('{}')
    key = run_scope.active_key()
    storage.sync_raw()
    manifest, first_digest = storage.read_manifest(store, storage.RAW_MANIFEST, key)
    storage.sync_raw()
    assert storage.read_manifest(store, storage.RAW_MANIFEST, key)[1] == first_digest
    object_key = next(entry['key'] for entry in manifest['files'] if entry['path'] == 'raw/source.nc')
    sample.write_bytes(b'second source')
    storage.sync_raw()
    assert store.objects[object_key] == b'first source'
    assert storage.read_manifest(store, storage.RAW_MANIFEST, key)[1] != first_digest

    monkeypatch.setattr(store, 'download_file', lambda bucket, object_key, target:
                        Path(target).write_bytes(store.objects[object_key]), raising=False)
    sample.unlink()
    storage.restore_raw()
    assert sample.read_bytes() == b'second source'
    evidence = json.loads((run_scope.receipt_path().parent / 'raw_restored.json').read_text())
    assert evidence['raw_manifest_sha256'] == storage.read_manifest(store, storage.RAW_MANIFEST, key)[1]


def test_guarded_publish_retries_committed_run_without_rebuilding(tmp_path, monkeypatch):
    from observatory import publish

    monkeypatch.setenv('OBSERVATORY_AIRFLOW_RUN_ID', 'scheduled__2024')
    monkeypatch.setattr(publish, 'DATA', tmp_path)
    key = run_scope.active_key()
    build = {'logical_date': '2024-01-01T00:00:00+00:00', 'site_list_sha256': 'site-hash',
             'years': [2024], 'sites': 1, 'dbt_invocation_id': 'build'}
    source = {'meta': {'site_ids': ['rabat'], 'site_count': 1, 'years': [2024],
                       'site_list_sha256': 'site-hash', 'built_at': build['logical_date']}}
    sources = {'source_snapshot.json': json.dumps(source).encode(), 'osm_downloads.json': b'{"files":[]}',
               'osm_services.geojson': b'{}', 'footprint.geojson': b'{}'}
    columns = ['site', 'year', 'month', 'hours', *promotion.METRICS]
    staged = {table: {'columns': ['id'], 'rows': [('rabat',)]} for table in GOLD_TABLES}
    staged['fact_climate_monthly'] = {'columns': columns, 'rows': [
        ('rabat', 2024, month, 24 * monthrange(2024, month)[1], 20, 20, .3) for month in range(1, 13)]}
    read_spark = Mock(return_value=staged)
    monkeypatch.setattr(publish, 'read_spark_tables', read_spark)
    monkeypatch.setattr(guardrails, 'load_build', Mock(return_value=(build, {}, b'{}', sources)))
    conn = Mock()
    conn.execute.return_value.fetchone.return_value = (None,)
    state = {}
    objects = {}
    remote = Mock()
    remote.get_object.side_effect = lambda *, Bucket, Key: {'Body': io.BytesIO(objects[Key])}
    remote.put_object.side_effect = lambda **call: objects.update({call['Key']: call['Body']})

    def put(_, path, bucket, key):
        body = path.read_bytes()
        objects[key] = body
        return {'key': key, 'bytes': len(body), 'sha256': run_scope.digest(body)}

    def commit(conn, staged, snapshot, assets, results, publication_id, receipt):
        assert promotion.CURRENT not in objects
        state.update(receipt)

    monkeypatch.setattr(publish, 'put_file', put)
    monkeypatch.setattr(publish, 'commit_tables', commit)
    monkeypatch.setattr(publish, 'current_promotion', lambda _: state or None)
    remote.put_object.side_effect = OSError('injected')
    with pytest.raises(OSError, match='injected'):
        publish.publish_guarded(conn, remote)
    assert state['publication_id'] == key and not state['complete']
    remote.put_object.side_effect = lambda **call: objects.update({call['Key']: call['Body']})
    publish.publish_guarded(conn, remote)
    read_spark.assert_called_once_with(run_scope.schema('gold'))
    assert json.loads(objects[promotion.CURRENT])['publication_id'] == key
