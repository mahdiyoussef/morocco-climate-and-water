with periods as (
  select *, case when month >= 10 then year + 1 else year end as water_year
  from {{ ref('fact_climate_monthly') }}
)
select site, water_year, count(distinct month) as months_covered,
       case when count(distinct month) = 12 then true else false end as is_complete,
       sum(rainfall_mm) as rainfall_mm,
       sum(temperature_c * hours) / sum(hours) as temperature_c,
       100.0 * sum(soil_moisture * hours) / sum(hours) as soil_moisture_pct,
       sum(hours) as hours
from periods group by site, water_year
