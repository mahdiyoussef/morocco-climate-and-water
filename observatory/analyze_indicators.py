"""Generate a reproducible indicator guide and profiles from the published Gold tables."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dashboard.data import load_bundle, aggregate_sites
from dashboard.indicators import annual_profiles, indicator_catalog


def main():
    bundle = load_bundle(ROOT / 'data')
    fact, anomalies = bundle['fact'], bundle['marts']['mart_monthly_anomalies']
    locations = bundle['locations'][['id', 'name']]
    profiles = annual_profiles(fact, anomalies).merge(locations, left_on='site', right_on='id').drop(columns='id')
    catalog = indicator_catalog()
    output = ROOT / 'docs'
    output.mkdir(exist_ok=True)
    profiles.to_csv(output / 'indicator_profiles_2015_2024.csv', index=False)
    catalog.to_csv(output / 'indicator_catalog.csv', index=False)
    latest = int(fact.year.max())
    current = aggregate_sites(fact[fact.year.eq(latest)])
    reference = aggregate_sites(fact[fact.year.between(2015, 2019)])
    rainfall = current.rainfall_mm.mean()
    baseline = reference.rainfall_mm.mean()
    change = 100 * (rainfall - baseline) / baseline
    temperature_change = current.temperature_c.mean() - reference.temperature_c.mean()
    moisture_change = current.soil_pct.mean() - reference.soil_pct.mean()
    latest_profiles = annual_profiles(fact[fact.year.eq(latest)], anomalies).merge(locations, left_on='site', right_on='id')
    warmest = latest_profiles.sort_values(['warm_dry_months_pct', 'name'], ascending=[False, True]).iloc[0]
    trends = bundle['marts']['mart_site_trends']
    table = '\n'.join('| ' + ' | '.join(str(value).replace('|', '/') for value in row) + ' |' for row in catalog.itertuples(index=False, name=None))
    guide = f"""# Indicators supported by the Morocco Gold dataset

This guide is calculated from the published Gold snapshot `{bundle['snapshot']['meta'].get('publication_id', 'local snapshot')}`.
The climate history covers 2015–2024: 12 nearby grid-cell samples, 120 months and 1,440 location-months.
All 24 indicators below can be calculated from the packaged data. The dashboard's **Indicators** workspace includes the catalog, formulas, annual profiles and CSV exports.

## Useful starting indicators

- **Rainfall departure (%):** locate months with rainfall below or above the same-month 2015–2019 reference.
- **Temperature departure (°C) and soil-moisture departure (percentage points):** describe changes in modeled surface conditions.
- **Complete October–September water-year rainfall (mm):** compare hydrological seasons while excluding incomplete boundary years.
- **Warmer and drier month frequency (%):** count simultaneous below-reference rain and above-reference temperature.
- **Wettest-three-month concentration (%) and annual rainfall CV (%):** describe seasonality and interannual variability.
- **Mapped water, healthcare, education objects and roads:** place climate samples in contemporary geographic context.

## Findings from the current snapshot

These comparisons give each sample location equal weight. They are not national averages.

- In {latest}, mean annual rainfall across the 12 samples was **{rainfall:.1f} mm**, compared with **{baseline:.1f} mm** for the 2015–2019 reference: **{change:+.1f}%**.
- Hour-weighted temperature at each location, then averaged across locations, was **{temperature_change:+.2f} °C** relative to the same reference period.
- Surface soil moisture changed by **{moisture_change:+.2f} percentage points** under the same weighting.
- **{warmest['name']}** had the highest {latest} warmer-and-drier frequency in the sample: **{warmest['warm_dry_months_pct']:.1f}%** of eligible months (ties resolved alphabetically).
- Descriptive 2015–2024 temperature slopes are positive at **{int(trends.temperature_slope_c_per_decade.gt(0).sum())} of {len(trends)}** sample locations. Rainfall slopes are negative at **{int(trends.rainfall_slope_mm_per_year.lt(0).sum())} of {len(trends)}** locations. Ten-year slopes are not forecasts or evidence of statistical significance.

The full-history profile export is `indicator_profiles_2015_2024.csv`. It includes all complete years, including reference years; use the dashboard's Years control to examine only 2020–2024 or another interval.

## Indicator catalog

| Family | Indicator | Unit | Gold source | Calculation | Scope and limits |
| --- | --- | --- | --- | --- | --- |
{table}

## Annual profile rules

Profiles include only location-years with exactly twelve distinct monthly observations. The Years and Locations filters apply; the Months and single Reference year controls do not change the annual profiles. The fixed anomaly reference is 2015–2019, always comparing the same calendar month at the same site.

Rainfall CV uses the sample standard deviation of annual totals and requires at least two complete years plus positive mean rainfall. Wettest-three-month concentration is computed separately within each complete year, then averaged; a zero-rain year has an undefined concentration. Condition frequencies exclude months with missing required reference values, and the profile export reports the eligible-month denominator. Equality with the reference is neither below nor above it.

## Additional indicators from the retained hourly ERA5 source

The 1,052,064 retained ERA5-Land hourly records can support additional Gold models: daily maximum/minimum of hourly temperature, daily temperature range, hours above a declared temperature threshold, heating/cooling degree-hours at an explicitly chosen base temperature, and hours below a stated surface-moisture reference. These are feasible extensions of the existing raw data, but are not included in this monthly deployment bundle. Heatwave definitions need an explicit method and suitable reference period.

## Indicators that need more data

Daily hot-day counts, heatwaves, consecutive dry days and rainfall-event intensity require daily or finer Gold observations. SPI/SPEI and robust climate normals need appropriate long historical series and methods; evapotranspiration is also needed for SPEI. Reservoir levels, groundwater, water-supply capacity, crop yields, irrigation demand, economic loss and population exposure are not present. OSM object counts cannot establish those quantities or a causal climate impact.

CHIRPS rainfall is estimated, and ERA5-Land variables are modeled. Soil moisture refers to the top 0–7 cm, not groundwater. Infrastructure is a contemporary OSM snapshot within 20 km, not a historical series. The Geofabrik rainfall footprint is not an authoritative national administrative boundary. Preserve the source attribution shown in the dashboard when sharing results.
"""
    (output / 'indicators.md').write_text(guide, encoding='utf-8')
    print(f'Analyzed {len(fact):,} location-months: {len(catalog)} indicators and {len(profiles)} location profiles.')
    print(f'{latest}: rainfall {rainfall:.1f} mm ({change:+.1f}%); temperature {temperature_change:+.2f} C; soil moisture {moisture_change:+.2f} pp.')


if __name__ == '__main__':
    main()
