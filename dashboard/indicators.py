"""Indicators supported by the published monthly Gold observations."""
import pandas as pd

# Formula, unit and scope travel with each indicator, including the CSV export.
CATALOG = [
    ('Rainfall', 'Period rainfall', 'mm', 'fact_climate_monthly', 'Sum months within each location-year; average period totals across years.', 'Selected months and years; each location is a grid-cell sample.'),
    ('Rainfall', 'Rainfall departure', '%', 'mart_monthly_anomalies', '100 × (rainfall − same-month reference mean) / reference mean.', '2015–2019 reference; undefined for a zero reference mean.'),
    ('Rainfall', 'Standardized rainfall departure', 'standard deviations', 'mart_monthly_anomalies', '(rainfall − same-month reference mean) / sample reference standard deviation.', 'Short reference period; this is not SPI.'),
    ('Rainfall', 'Below-reference rainfall months', '% of eligible months', 'mart_monthly_anomalies', '100 × count(rainfall < same-month reference mean) / eligible months.', 'Exploratory frequency; no official drought threshold.'),
    ('Rainfall', 'Wettest-three-month concentration', '% of annual rainfall', 'fact_climate_monthly', '100 × sum(the three largest monthly totals) / full-year rainfall; average across complete years.', 'Monthly concentration, not daily rainfall intensity; zero-rain years are undefined.'),
    ('Rainfall', 'Interannual rainfall variability', '% CV', 'fact_climate_monthly', '100 × sample standard deviation of complete annual totals / mean annual total.', 'At least two complete years and positive mean rainfall; descriptive variability.'),
    ('Rainfall', 'Water-year rainfall', 'mm', 'mart_water_year', 'Sum October–September rainfall, labeled by the ending year.', 'Use is_complete=true for comparisons; boundary years are incomplete.'),
    ('Rainfall', 'Full-footprint rainfall', 'mm/month', 'mart_spatial_rainfall', 'Sum(cell rainfall × cos(latitude)) / sum(cos(latitude)).', 'Geofabrik extract footprint; independent of the twelve sample locations.'),
    ('Rainfall', 'Rainfall trend', 'mm/year', 'mart_site_trends', 'OLS slope of ten complete annual rainfall totals against year.', '2015–2024; descriptive slope without significance tests or forecasts.'),
    ('Temperature', 'Mean temperature', '°C', 'fact_climate_monthly', 'Sum(monthly temperature × hourly count) / sum(hourly count).', '2 m modeled temperature; weight by hours within each location.'),
    ('Temperature', 'Temperature departure', '°C', 'mart_monthly_anomalies', 'Monthly mean temperature − same-month 2015–2019 reference temperature.', 'The reference temperature is hour-weighted.'),
    ('Temperature', 'Warmer-than-reference months', '% of eligible months', 'mart_monthly_anomalies', '100 × count(temperature > same-month reference) / eligible months.', 'Monthly means; does not measure hot days or heatwaves.'),
    ('Temperature', 'Temperature trend', '°C/decade', 'mart_site_trends', '10 × OLS slope of hour-weighted annual mean temperature against year.', 'Ten-year exploratory slope, not an attribution or projection.'),
    ('Soil & compound conditions', 'Surface soil moisture', '% vol.', 'fact_climate_monthly', '100 × hour-weighted mean of soil_moisture.', 'Modeled volumetric moisture at 0–7 cm; not groundwater or reservoir storage.'),
    ('Soil & compound conditions', 'Soil moisture departure', 'percentage points', 'mart_monthly_anomalies', '100 × (soil_moisture − same-month reference moisture).', 'Percentage-point difference, not percentage change.'),
    ('Soil & compound conditions', 'Below-reference soil moisture months', '% of eligible months', 'mart_monthly_anomalies', '100 × count(soil_moisture < same-month reference) / eligible months.', 'Exploratory surface-moisture condition, not agricultural drought classification.'),
    ('Soil & compound conditions', 'Warmer and drier months', '% of eligible months', 'mart_monthly_anomalies', '100 × count(rainfall < reference AND temperature > reference) / eligible months.', 'Both comparisons use the same calendar month at the same location; no causal claim.'),
    ('Mapped infrastructure', 'Mapped water objects', 'objects', 'fact_infrastructure', 'Mapped water-service objects within 20 km of each sample location.', 'Contemporary OSM context; not supply, storage volume or service capacity.'),
    ('Mapped infrastructure', 'Mapped healthcare objects', 'objects', 'fact_infrastructure', 'Mapped healthcare-service objects within 20 km.', 'Mapping completeness varies; counts are not hospital beds or accessibility.'),
    ('Mapped infrastructure', 'Mapped education objects', 'objects', 'fact_infrastructure', 'Mapped education-service objects within 20 km.', 'Counts are not enrollment or educational outcomes.'),
    ('Mapped infrastructure', 'Mapped road length', 'km', 'fact_infrastructure', 'Sum road-segment lengths whose midpoint lies within 20 km.', 'Approximate neighborhood context; no exact clipping or road-capacity claim.'),
    ('Mapped infrastructure', 'Unique mapped services', 'unique objects', 'osm.geojson', 'Count distinct osm_id values among selected location memberships.', 'Deduplicate overlapping neighborhoods; do not sum overlapping location counts.'),
    ('Coverage & quality', 'Location-month coverage', '%', 'fact_climate_monthly + dimensions', '100 × observed unique location-month keys / expected keys.', 'The published dataset has twelve samples × 120 months; not national coverage.'),
    ('Coverage & quality', 'Hourly source completeness', '%', 'fact_climate_monthly', '100 × observed hours / leap-year-aware expected monthly hours.', 'Source completeness is not measurement accuracy.'),
]


def indicator_catalog():
    return pd.DataFrame(CATALOG, columns=['Family', 'Indicator', 'Unit', 'Gold source', 'Calculation', 'Scope and limits'])


def annual_profiles(fact, anomalies):
    """Full-year profiles: incomplete years never become annual totals or CV inputs."""
    fields = ['site', 'complete_years', 'mean_annual_rainfall_mm', 'rainfall_cv_pct',
              'wettest_three_month_share_pct', 'eligible_months', 'below_reference_rainfall_months_pct',
              'warmer_reference_months_pct', 'below_reference_moisture_months_pct', 'warm_dry_months_pct']
    full = fact.groupby(['site', 'year'], group_keys=False).filter(
        lambda group: len(group) == 12 and group['month'].nunique() == 12)
    if full.empty:
        return pd.DataFrame(columns=fields)
    annual = full.groupby(['site', 'year']).agg(
        rainfall_mm=('rainfall_mm', 'sum'), top_three_mm=('rainfall_mm', lambda values: values.nlargest(3).sum()))
    annual['concentration_pct'] = 100 * annual.top_three_mm / annual.rainfall_mm.where(annual.rainfall_mm.gt(0))
    profiles = annual.groupby('site').agg(
        complete_years=('rainfall_mm', 'size'), mean_annual_rainfall_mm=('rainfall_mm', 'mean'),
        annual_std=('rainfall_mm', 'std'), wettest_three_month_share_pct=('concentration_pct', 'mean'))
    profiles['rainfall_cv_pct'] = 100 * profiles.annual_std / profiles.mean_annual_rainfall_mm.where(profiles.mean_annual_rainfall_mm.gt(0))
    eligible = anomalies.merge(full[['site', 'year']].drop_duplicates(), on=['site', 'year'], validate='many_to_one')
    required = ['rainfall_mm', 'baseline_rainfall_mm', 'temperature_c', 'baseline_temperature_c',
                'soil_moisture', 'baseline_soil_moisture']
    eligible = eligible.dropna(subset=required).copy()
    eligible['below_reference_rainfall_months_pct'] = eligible.rainfall_mm.lt(eligible.baseline_rainfall_mm)
    eligible['warmer_reference_months_pct'] = eligible.temperature_c.gt(eligible.baseline_temperature_c)
    eligible['below_reference_moisture_months_pct'] = eligible.soil_moisture.lt(eligible.baseline_soil_moisture)
    eligible['warm_dry_months_pct'] = eligible.below_reference_rainfall_months_pct & eligible.warmer_reference_months_pct
    rates = [name for name in fields if name.endswith('months_pct')]
    conditions = eligible.groupby('site')[rates].mean() * 100
    conditions['eligible_months'] = eligible.groupby('site').size()
    profiles = profiles.drop(columns='annual_std').join(conditions).reset_index()
    return profiles[fields]
