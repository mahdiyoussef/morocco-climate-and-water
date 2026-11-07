with annual as (
  select site, year, sum(rainfall_mm) as rainfall_mm,
         sum(temperature_c * hours) / sum(hours) as temperature_c
  from {{ ref('fact_climate_monthly') }} group by site, year
  having count(distinct month) = 12
), sums as (
  select site, count(*) as observation_years, min(year) as first_year, max(year) as last_year,
         sum(year * 1.0) as sx, sum(year * year * 1.0) as sxx,
         sum(rainfall_mm) as sy_rain, sum(year * rainfall_mm) as sxy_rain,
         sum(temperature_c) as sy_temp, sum(year * temperature_c) as sxy_temp
  from annual group by site
)
select site, observation_years, first_year, last_year,
       (observation_years * sxy_rain - sx * sy_rain) / nullif(observation_years * sxx - sx * sx, 0) as rainfall_slope_mm_per_year,
       10.0 * (observation_years * sxy_temp - sx * sy_temp) / nullif(observation_years * sxx - sx * sx, 0) as temperature_slope_c_per_decade
from sums
