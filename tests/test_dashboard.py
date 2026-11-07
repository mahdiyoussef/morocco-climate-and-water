"""Exercise real Gold data, aggregation semantics, and all application pages."""
import numpy as np
import pandas as pd
import pytest
from pathlib import Path
from streamlit.testing.v1 import AppTest
from dashboard.data import DEFAULT_DATA_ROOT, aggregate_sites, comparison_table, filter_climate, load_bundle


@pytest.fixture(scope='module')
def bundle():
    return load_bundle(DEFAULT_DATA_ROOT)


def test_real_coverage_and_matched_reference(bundle):
    fact = bundle['fact']
    assert len(fact) == 1440 and fact.site.nunique() == 12
    assert len(bundle['marts']) == 4
    selected = filter_climate(fact, ['rabat'], (2024, 2024), [1, 2])
    comparison = comparison_table(bundle, selected, 2015, [1, 2]).iloc[0]
    reference = filter_climate(fact, ['rabat'], (2015, 2015), [1, 2])
    assert comparison.rainfall_mm == pytest.approx(selected.rainfall_mm.sum())
    assert comparison.rain_change_pct == pytest.approx(100 * (selected.rainfall_mm.sum() / reference.rainfall_mm.sum() - 1))


def test_temperature_is_hour_weighted_and_rainfall_is_mean_year_total():
    data = pd.DataFrame({'site': ['x'] * 3, 'year': [2023, 2024, 2024], 'month': [1, 1, 2],
                         'rainfall_mm': [60., 20., 80.], 'temperature_c': [0., 10., 20.],
                         'soil_pct': [10., 20., 40.], 'hours': [100, 100, 200]})
    row = aggregate_sites(data).iloc[0]
    assert row.rainfall_mm == 80 and row.temperature_c == 12.5 and row.soil_pct == 27.5


def test_zero_reference_rainfall_is_undefined(bundle):
    altered = {**bundle, 'fact': bundle['fact'].copy()}
    altered['fact'].loc[altered['fact'].year.eq(2015), 'rainfall_mm'] = 0
    selected = filter_climate(altered['fact'], ['rabat'], (2024, 2024), [7])
    assert np.isnan(comparison_table(altered, selected, 2015, [7]).iloc[0].rain_change_pct)


def test_all_pages_and_filters(monkeypatch):
    monkeypatch.setenv('OBSERVATORY_DATA_BACKEND', 'local')
    app = AppTest.from_file(Path(__file__).resolve().parents[1] / 'streamlit_app.py', default_timeout=90).run()
    assert not app.exception
    app.toggle(key='online_map').set_value(False).run()
    assert not app.exception
    for page in app.radio(key='page').options:
        app.radio(key='page').set_value(page).run()
        assert not app.exception, (page, app.exception)
    app.radio(key='page').set_value('Overview').run()
    app.multiselect(key='locations').set_value([]).run()
    assert not app.exception
    assert any('Choose at least' in item.value for item in app.info)
    app.button[0].click().run()
    assert not app.exception
    assert len(app.multiselect(key='locations').value) == 12
    app.multiselect(key='locations').set_value(['rabat']).run()
    app.multiselect(key='months').set_value([1, 2]).run()
    app.slider(key='years').set_value((2020, 2024)).run()
    assert not app.exception
    app.multiselect(key='months').set_value([]).run()
    assert not app.exception


def test_indicator_profiles_ignore_month_selection(monkeypatch):
    monkeypatch.setenv('OBSERVATORY_DATA_BACKEND', 'local')
    app = AppTest.from_file(Path(__file__).resolve().parents[1] / 'streamlit_app.py', default_timeout=90).run()
    app.radio(key='page').set_value('Indicators').run()
    app.slider(key='years').set_value((2015, 2024)).run()
    before = next(metric.value for metric in app.metric if metric.label == 'Annual rainfall variability')
    app.multiselect(key='months').set_value([]).run()
    assert not app.exception and not app.error
    assert next(metric.value for metric in app.metric if metric.label == 'Annual rainfall variability') == before
    app.multiselect(key='locations').set_value([]).run()
    assert any('Choose at least one location' in item.value for item in app.info)
