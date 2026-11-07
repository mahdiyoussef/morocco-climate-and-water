with baseline as (
  select site, month, count(*) as baseline_years,
         avg(rainfall_mm) as baseline_rainfall_mm,
         stddev_samp(rainfall_mm) as rainfall_stddev_mm,
         sum(temperature_c * hours) / sum(hours) as baseline_temperature_c,
         sum(soil_moisture * hours) / sum(hours) as baseline_soil_moisture
  from {{ ref('fact_climate_monthly') }}
  where year between {{ var('baseline_start_year') }} and {{ var('baseline_end_year') }}
  group by site, month
)
select f.*, b.baseline_years, b.baseline_rainfall_mm, b.baseline_temperature_c,
       b.baseline_soil_moisture,
       100.0 * (f.rainfall_mm - b.baseline_rainfall_mm) / nullif(b.baseline_rainfall_mm, 0) as rainfall_anomaly_pct,
       (f.rainfall_mm - b.baseline_rainfall_mm) / nullif(b.rainfall_stddev_mm, 0) as rainfall_standardized_departure,
       f.temperature_c - b.baseline_temperature_c as temperature_anomaly_c,
       100.0 * (f.soil_moisture - b.baseline_soil_moisture) as soil_moisture_anomaly_pp,
       {{ var('baseline_start_year') }} as baseline_start_year,
       {{ var('baseline_end_year') }} as baseline_end_year
from {{ ref('fact_climate_monthly') }} f join baseline b on f.site = b.site and f.month = b.month
