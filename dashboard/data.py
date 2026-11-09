"""Load the shared Gold tables and calculate explicitly scoped climate comparisons."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_ROOT = ROOT / 'data'
REQUIRED = ('fact_climate_monthly.csv', 'dim_location.csv', 'dim_month.csv', 'fact_infrastructure.csv', 'snapshot.json')
MEASURES = {
    'Rainfall': ('rainfall_mm', 'mm', '#19745B'),
    'Temperature': ('temperature_c', '°C', '#BC8052'),
    'Soil moisture': ('soil_pct', '% vol.', '#518AA3'),
}


def file_signature(root: Path) -> tuple:
    """Invalidate the UI cache when a published input changes."""
    files = list((root / 'gold').glob('*')) + list((root / 'metadata').glob('*.json'))
    return tuple(sorted((str(p), p.stat().st_mtime_ns, p.stat().st_size) for p in files if p.is_file()))


def load_bundle(root: Path) -> dict:
    gold = root / 'gold'
    pending = gold / '.promotion-pending'
    if pending.exists():
        raise ValueError('Local Gold publication is awaiting recovery')
    signature = file_signature(root)
    missing = [name for name in REQUIRED if not (gold / name).is_file()]
    if missing:
        raise ValueError('Missing processed data: ' + ', '.join(missing) + '. Run observatory/warehouse.py local-all first.')
    snapshot_path = gold / 'snapshot.json'
    snapshot_bytes = snapshot_path.read_bytes()
    snapshot = json.loads(snapshot_bytes)
    tables = {p.stem: pd.read_csv(p, dtype={'site': str, 'year_month': str}) for p in gold.glob('*.csv')}
    metadata = {}
    for key, filename in [('acquisition', 'climate_downloads.json'), ('era5', 'era5_downloads.json'),
                          ('verification', 'processing_verification.json'), ('dbt', 'dbt_run_results.json')]:
        path = root / 'metadata' / filename
        metadata[key] = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
    if pending.exists() or file_signature(root) != signature or snapshot_path.read_bytes() != snapshot_bytes:
        raise ValueError('Local Gold changed while loading. Retry after publication completes.')
    return build_bundle(tables, snapshot, metadata, root)


def build_bundle(tables, snapshot, metadata, root, assets=None):
    fact = tables['fact_climate_monthly'].copy()
    locations = tables['dim_location'].copy()
    months = tables['dim_month'].copy()
    infrastructure = tables['fact_infrastructure'].copy()
    fact['year_month'] = fact['year_month'].astype(str)
    months['year_month'] = months['year_month'].astype(str)
    needed = {'site', 'year', 'month', 'year_month', 'rainfall_mm', 'temperature_c', 'soil_moisture', 'hours'}
    if not needed.issubset(fact.columns):
        raise ValueError('The climate table is missing required columns: ' + ', '.join(sorted(needed - set(fact))))
    if locations['id'].duplicated().any() or months['year_month'].duplicated().any() or infrastructure['site'].duplicated().any():
        raise ValueError('A dimension or infrastructure table has duplicate location/month keys.')
    if fact.duplicated(['site', 'year_month']).any():
        raise ValueError('Climate data has duplicate location-month records.')
    if not set(fact['site']) <= set(locations['id']) or not set(fact['year_month']) <= set(months['year_month']):
        raise ValueError('Some climate records have no matching location or calendar month.')
    if set(infrastructure['site']) != set(locations['id']):
        raise ValueError('Infrastructure coverage does not match the location dimension.')
    numeric = ['rainfall_mm', 'temperature_c', 'soil_moisture', 'hours']
    if not np.isfinite(fact[numeric].to_numpy(dtype=float)).all():
        raise ValueError('Climate measurements contain missing or non-finite values.')
    if not (fact['rainfall_mm'].ge(0).all() and fact['soil_moisture'].between(0, 1).all()
            and fact['hours'].gt(0).all() and fact['month'].between(1, 12).all()):
        raise ValueError('Climate measurements fall outside their expected ranges.')
    encoded_period = fact['year'].astype(str) + fact['month'].astype(str).str.zfill(2)
    if not encoded_period.eq(fact['year_month']).all():
        raise ValueError('Calendar keys disagree with climate record dates.')
    if not np.isfinite(locations[['lat', 'lon']].to_numpy()).all():
        raise ValueError('Location coordinates contain missing values.')
    if not locations['lat'].between(-90, 90).all() or not locations['lon'].between(-180, 180).all():
        raise ValueError('Location coordinates are invalid.')
    fact['date'] = pd.to_datetime(dict(year=fact['year'], month=fact['month'], day=1))
    fact['soil_pct'] = fact['soil_moisture'] * 100
    fact = fact.merge(locations, left_on='site', right_on='id', validate='many_to_one').drop(columns='id')
    if fact['site'].nunique() * len(months) != len(fact):
        raise ValueError('The monthly climate table has incomplete location/calendar coverage.')
    audit = tables.get('source_audit', pd.DataFrame())
    marts = {name: tables[name] for name in ['mart_monthly_anomalies', 'mart_water_year', 'mart_site_trends', 'mart_spatial_rainfall'] if name in tables}
    return {'fact': fact.sort_values(['date', 'site']).reset_index(drop=True), 'locations': locations, 'marts': marts,
            'months': months, 'infrastructure': infrastructure, 'snapshot': snapshot, 'audit': audit,
            'metadata': metadata, 'root': str(root), 'assets': assets, 'backend': 'local'}


def filter_climate(frame: pd.DataFrame, sites, years, months) -> pd.DataFrame:
    return frame[frame['site'].isin(sites) & frame['year'].between(*years) & frame['month'].isin(months)].copy()


def aggregate_sites(frame: pd.DataFrame) -> pd.DataFrame:
    """Rain: mean period totals across years. Land variables: hour-weighted per site."""
    columns = ['site', 'rainfall_mm', 'temperature_c', 'soil_pct', 'hours', 'location_months', 'years']
    if frame.empty:
        return pd.DataFrame(columns=columns)
    values = frame.assign(temp_hours=frame['temperature_c'] * frame['hours'],
                          soil_hours=frame['soil_pct'] * frame['hours'])
    grouped = values.groupby('site', sort=False).agg(
        temp_hours=('temp_hours', 'sum'), soil_hours=('soil_hours', 'sum'), hours=('hours', 'sum'),
        location_months=('month', 'size'), years=('year', 'nunique'))
    rain = frame.groupby(['site', 'year'])['rainfall_mm'].sum().groupby('site').mean()
    grouped['rainfall_mm'] = rain
    grouped['temperature_c'] = grouped['temp_hours'] / grouped['hours']
    grouped['soil_pct'] = grouped['soil_hours'] / grouped['hours']
    return grouped.reset_index()[columns]


def comparison_table(bundle, selected: pd.DataFrame, reference_year: int, selected_months) -> pd.DataFrame:
    ids = selected['site'].unique()
    baseline = bundle['fact'][bundle['fact']['site'].isin(ids) & bundle['fact']['year'].eq(reference_year)
                              & bundle['fact']['month'].isin(selected_months)]
    current = aggregate_sites(selected)
    reference = aggregate_sites(baseline)[['site', 'rainfall_mm', 'temperature_c', 'soil_pct']].rename(
        columns={name: f'reference_{name}' for name in ('rainfall_mm', 'temperature_c', 'soil_pct')})
    table = current.merge(reference, on='site', how='left', validate='one_to_one')
    table['rain_change_pct'] = 100 * (table['rainfall_mm'] - table['reference_rainfall_mm']) / table['reference_rainfall_mm'].where(table['reference_rainfall_mm'].gt(0))
    table['temp_delta_c'] = table['temperature_c'] - table['reference_temperature_c']
    table['soil_delta_pp'] = table['soil_pct'] - table['reference_soil_pct']
    table = table.merge(bundle['locations'], left_on='site', right_on='id', validate='one_to_one').drop(columns='id')
    return table.merge(bundle['infrastructure'], on='site', validate='one_to_one').sort_values('rainfall_mm', ascending=False)


def cohort_summary(table: pd.DataFrame) -> dict:
    current = table[['rainfall_mm', 'temperature_c', 'soil_pct']].mean()
    ref = table[['reference_rainfall_mm', 'reference_temperature_c', 'reference_soil_pct']].mean()
    return {'rainfall_mm': current['rainfall_mm'], 'temperature_c': current['temperature_c'], 'soil_pct': current['soil_pct'],
            'rain_change_pct': 100 * (current['rainfall_mm'] - ref['reference_rainfall_mm']) / ref['reference_rainfall_mm']
            if ref['reference_rainfall_mm'] > 0 else None,
            'temp_delta_c': current['temperature_c'] - ref['reference_temperature_c'],
            'soil_delta_pp': current['soil_pct'] - ref['reference_soil_pct']}


def monthly_series(frame: pd.DataFrame, metric: str) -> pd.Series:
    if metric == 'rainfall_mm':
        return frame.groupby('date')[metric].mean().sort_index()
    weighted = frame.assign(weighted=frame[metric] * frame['hours']).groupby('date').agg(
        weighted=('weighted', 'sum'), hours=('hours', 'sum'))
    return (weighted['weighted'] / weighted['hours']).sort_index()


def assets_for_sites(geojson: dict, sites, categories=None) -> list:
    selected = set(sites)
    allowed = set(categories or ['water', 'healthcare', 'education'])
    return [feature for feature in geojson.get('features', [])
            if feature.get('properties', {}).get('category') in allowed
            and selected.intersection(feature.get('properties', {}).get('sites', []))]


def csv_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False).encode('utf-8-sig')
