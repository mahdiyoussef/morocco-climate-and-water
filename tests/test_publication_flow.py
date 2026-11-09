"""Exercise the distributed publication entry point without live services."""
import json
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from observatory import publish
from observatory.warehouse import GOLD_TABLES


@pytest.mark.parametrize('failure', [None, 'dbt', 'postgres'])
def test_spark_publication_advances_pointer_only_after_verified_database_commit(tmp_path, monkeypatch, failure):
    results = {
        'args': {'target': 'spark', 'which': 'build'},
        'metadata': {'invocation_id': 'build-id'},
        'results': [
            *[{'unique_id': f'model.morocco.{table}', 'status': 'success'} for table in GOLD_TABLES],
            {'unique_id': 'test.morocco.quality', 'status': 'fail' if failure == 'dbt' else 'pass'},
        ],
    }
    metadata = tmp_path / 'metadata'
    metadata.mkdir()
    results_file = metadata / 'dbt_run_results.json'
    results_file.write_text(json.dumps(results))
    (metadata / 'source_snapshot.json').write_text('{"meta": {"built_at": "2024-01-01"}}')
    (metadata / 'osm_downloads.json').write_text('{"files": []}')
    processed = tmp_path / 'processed'
    processed.mkdir()
    for name in ('osm_services', 'footprint'):
        (processed / f'{name}.geojson').write_text('{}')

    monkeypatch.setattr(publish, 'DATA', tmp_path)
    monkeypatch.setenv('DBT_RUN_RESULTS', str(results_file))
    monkeypatch.setenv('OBSERVATORY_RUN_ID', 'quality-test')
    connect = MagicMock()
    cursor = connect.return_value.cursor.return_value
    cursor.description = [('site',)]
    cursor.fetchall.side_effect = [(('site', 'string'),), (('rabat',),)] * len(GOLD_TABLES)
    monkeypatch.setitem(sys.modules, 'pyhive', SimpleNamespace(hive=SimpleNamespace(Connection=connect)))
    store = MagicMock()
    monkeypatch.setattr(publish, 'client', lambda: store)
    monkeypatch.setattr(publish, 'put_file', lambda _, path, bucket, key: {'key': key})
    monkeypatch.setattr(publish.socket, 'socket', MagicMock())

    def commit(staged, snapshot, assets, build, invocation):
        assert set(staged) == set(GOLD_TABLES)
        assert invocation == snapshot['meta']['publication_id'] == 'quality-test'
        assert build == results
        assert not store.put_object.called
        if failure == 'postgres':
            raise RuntimeError('Database transaction failed')

    monkeypatch.setattr(publish, 'publish_tables', commit)
    if failure:
        with pytest.raises((ValueError, RuntimeError), match='must pass|transaction failed'):
            publish.main()
        assert not store.put_object.called
        assert not (metadata / 'publication.json').exists()
        if failure == 'dbt':
            connect.assert_not_called()
    else:
        publish.main()
        store.put_object.assert_called_once()
        call = store.put_object.call_args.kwargs
        assert call['Key'] == 'observatory/current.json'
        assert json.loads(call['Body'])['publication_id'] == 'quality-test'
        assert (tmp_path / 'gold/fact_climate_monthly.csv').read_text() == 'site\nrabat\n'
        assert json.loads((metadata / 'publication.json').read_text())['gold_rows'] == 1
