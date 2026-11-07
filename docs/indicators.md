# Indicators supported by the Morocco Gold dataset

This guide is calculated from the published Gold snapshot `44c63fc5134047d39e63f48dc115b37b`.
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

- In 2024, mean annual rainfall across the 12 samples was **309.0 mm**, compared with **338.4 mm** for the 2015–2019 reference: **-8.7%**.
- Hour-weighted temperature at each location, then averaged across locations, was **+1.02 °C** relative to the same reference period.
- Surface soil moisture changed by **-1.63 percentage points** under the same weighting.
- **Beni Mellal** had the highest 2024 warmer-and-drier frequency in the sample: **75.0%** of eligible months (ties resolved alphabetically).
- Descriptive 2015–2024 temperature slopes are positive at **12 of 12** sample locations. Rainfall slopes are negative at **9 of 12** locations. Ten-year slopes are not forecasts or evidence of statistical significance.

The full-history profile export is `indicator_profiles_2015_2024.csv`. It includes all complete years, including reference years; use the dashboard's Years control to examine only 2020–2024 or another interval.

## Indicator catalog

| Family | Indicator | Unit | Gold source | Calculation | Scope and limits |
| --- | --- | --- | --- | --- | --- |
| Rainfall | Period rainfall | mm | fact_climate_monthly | Sum months within each location-year; average period totals across years. | Selected months and years; each location is a grid-cell sample. |
| Rainfall | Rainfall departure | % | mart_monthly_anomalies | 100 × (rainfall − same-month reference mean) / reference mean. | 2015–2019 reference; undefined for a zero reference mean. |
| Rainfall | Standardized rainfall departure | standard deviations | mart_monthly_anomalies | (rainfall − same-month reference mean) / sample reference standard deviation. | Short reference period; this is not SPI. |
| Rainfall | Below-reference rainfall months | % of eligible months | mart_monthly_anomalies | 100 × count(rainfall < same-month reference mean) / eligible months. | Exploratory frequency; no official drought threshold. |
| Rainfall | Wettest-three-month concentration | % of annual rainfall | fact_climate_monthly | 100 × sum(the three largest monthly totals) / full-year rainfall; average across complete years. | Monthly concentration, not daily rainfall intensity; zero-rain years are undefined. |
| Rainfall | Interannual rainfall variability | % CV | fact_climate_monthly | 100 × sample standard deviation of complete annual totals / mean annual total. | At least two complete years and positive mean rainfall; descriptive variability. |
| Rainfall | Water-year rainfall | mm | mart_water_year | Sum October–September rainfall, labeled by the ending year. | Use is_complete=true for comparisons; boundary years are incomplete. |
| Rainfall | Full-footprint rainfall | mm/month | mart_spatial_rainfall | Sum(cell rainfall × cos(latitude)) / sum(cos(latitude)). | Geofabrik extract footprint; independent of the twelve sample locations. |
| Rainfall | Rainfall trend | mm/year | mart_site_trends | OLS slope of ten complete annual rainfall totals against year. | 2015–2024; descriptive slope without significance tests or forecasts. |
| Temperature | Mean temperature | °C | fact_climate_monthly | Sum(monthly temperature × hourly count) / sum(hourly count). | 2 m modeled temperature; weight by hours within each location. |
| Temperature | Temperature departure | °C | mart_monthly_anomalies | Monthly mean temperature − same-month 2015–2019 reference temperature. | The reference temperature is hour-weighted. |
| Temperature | Warmer-than-reference months | % of eligible months | mart_monthly_anomalies | 100 × count(temperature > same-month reference) / eligible months. | Monthly means; does not measure hot days or heatwaves. |
| Temperature | Temperature trend | °C/decade | mart_site_trends | 10 × OLS slope of hour-weighted annual mean temperature against year. | Ten-year exploratory slope, not an attribution or projection. |
| Soil & compound conditions | Surface soil moisture | % vol. | fact_climate_monthly | 100 × hour-weighted mean of soil_moisture. | Modeled volumetric moisture at 0–7 cm; not groundwater or reservoir storage. |
| Soil & compound conditions | Soil moisture departure | percentage points | mart_monthly_anomalies | 100 × (soil_moisture − same-month reference moisture). | Percentage-point difference, not percentage change. |
| Soil & compound conditions | Below-reference soil moisture months | % of eligible months | mart_monthly_anomalies | 100 × count(soil_moisture < same-month reference) / eligible months. | Exploratory surface-moisture condition, not agricultural drought classification. |
| Soil & compound conditions | Warmer and drier months | % of eligible months | mart_monthly_anomalies | 100 × count(rainfall < reference AND temperature > reference) / eligible months. | Both comparisons use the same calendar month at the same location; no causal claim. |
| Mapped infrastructure | Mapped water objects | objects | fact_infrastructure | Mapped water-service objects within 20 km of each sample location. | Contemporary OSM context; not supply, storage volume or service capacity. |
| Mapped infrastructure | Mapped healthcare objects | objects | fact_infrastructure | Mapped healthcare-service objects within 20 km. | Mapping completeness varies; counts are not hospital beds or accessibility. |
| Mapped infrastructure | Mapped education objects | objects | fact_infrastructure | Mapped education-service objects within 20 km. | Counts are not enrollment or educational outcomes. |
| Mapped infrastructure | Mapped road length | km | fact_infrastructure | Sum road-segment lengths whose midpoint lies within 20 km. | Approximate neighborhood context; no exact clipping or road-capacity claim. |
| Mapped infrastructure | Unique mapped services | unique objects | osm.geojson | Count distinct osm_id values among selected location memberships. | Deduplicate overlapping neighborhoods; do not sum overlapping location counts. |
| Coverage & quality | Location-month coverage | % | fact_climate_monthly + dimensions | 100 × observed unique location-month keys / expected keys. | The published dataset has twelve samples × 120 months; not national coverage. |
| Coverage & quality | Hourly source completeness | % | fact_climate_monthly | 100 × observed hours / leap-year-aware expected monthly hours. | Source completeness is not measurement accuracy. |

## Annual profile rules

Profiles include only location-years with exactly twelve distinct monthly observations. The Years and Locations filters apply; the Months and single Reference year controls do not change the annual profiles. The fixed anomaly reference is 2015–2019, always comparing the same calendar month at the same site.

Rainfall CV uses the sample standard deviation of annual totals and requires at least two complete years plus positive mean rainfall. Wettest-three-month concentration is computed separately within each complete year, then averaged; a zero-rain year has an undefined concentration. Condition frequencies exclude months with missing required reference values, and the profile export reports the eligible-month denominator. Equality with the reference is neither below nor above it.

## Additional indicators from the retained hourly ERA5 source

The 1,052,064 retained ERA5-Land hourly records can support additional Gold models: daily maximum/minimum of hourly temperature, daily temperature range, hours above a declared temperature threshold, heating/cooling degree-hours at an explicitly chosen base temperature, and hours below a stated surface-moisture reference. These are feasible extensions of the existing raw data, but are not included in this monthly deployment bundle. Heatwave definitions need an explicit method and suitable reference period.

## Indicators that need more data

Daily hot-day counts, heatwaves, consecutive dry days and rainfall-event intensity require daily or finer Gold observations. SPI/SPEI and robust climate normals need appropriate long historical series and methods; evapotranspiration is also needed for SPEI. Reservoir levels, groundwater, water-supply capacity, crop yields, irrigation demand, economic loss and population exposure are not present. OSM object counts cannot establish those quantities or a causal climate impact.

CHIRPS rainfall is estimated, and ERA5-Land variables are modeled. Soil moisture refers to the top 0–7 cm, not groundwater. Infrastructure is a contemporary OSM snapshot within 20 km, not a historical series. The Geofabrik rainfall footprint is not an authoritative national administrative boundary. Preserve the source attribution shown in the dashboard when sharing results.
