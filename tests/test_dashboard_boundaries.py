"""Cover dashboard storage failures and the map and export contracts."""
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from dashboard import charts, postgres_backend
from dashboard.data import assets_for_sites, csv_bytes


@pytest.mark.parametrize('marker', [None, ('version', None, {'dim_location': 2})])
def test_postgres_reader_rejects_missing_or_inconsistent_publication(monkeypatch, marker):
    conn = Mock()
    table = SimpleNamespace(description=[SimpleNamespace(name='id')], fetchall=lambda: [('rabat',)])

    def execute(statement, parameters=None):
        if isinstance(statement, str):
            if 'from gold.publication' in statement:
                return SimpleNamespace(fetchone=lambda: marker)
            return None
        return table

    conn.execute.side_effect = execute
    monkeypatch.setattr(postgres_backend, 'connection', lambda: nullcontext(conn))
    with pytest.raises(ValueError, match='Publish a tested|row counts do not reconcile'):
        postgres_backend.load_postgres_bundle()


def test_offline_map_keeps_points_and_never_configures_carto():
    table = pd.DataFrame([{'name': 'Rabat', 'lat': 34.0, 'lon': -6.8, 'rainfall_mm': 10}])
    deck = charts.climate_map(table, 'rainfall_mm', 'mm', online=False)
    assert 'carto' not in deck.to_json().lower()
    assert any(layer.type == 'ScatterplotLayer' for layer in deck.layers)


def test_infrastructure_selection_and_download():
    assets = {'features': [
        {'properties': {'category': 'water', 'sites': ['rabat', 'fes']}},
        {'properties': {'category': 'education', 'sites': ['rabat']}},
        {'properties': {'category': 'water', 'sites': ['marrakech']}},
    ]}
    assert assets_for_sites(assets, ['rabat', 'fes'], ['water']) == assets['features'][:1]
    assert csv_bytes(pd.DataFrame({'city': ['Fès']})).decode('utf-8-sig').splitlines() == ['city', 'Fès']


def test_local_dashboard_blocks_partially_replaced_exports(tmp_path):
    from dashboard.data import load_bundle

    gold = tmp_path / 'gold'
    gold.mkdir()
    (gold / '.promotion-pending').write_text('{"publication_id":"pending"}')
    with pytest.raises(ValueError, match='awaiting recovery'):
        load_bundle(tmp_path)
