with hourly as (
  select site, year, month,
         round(avg(temperature_c), 3) as temperature_c,
         round(avg(soil_moisture), 5) as soil_moisture,
         count(*) as hours
  from {{ ref('stg_climate_hourly') }}
  group by site, year, month
), source_coordinates as (
  select id as site, era5_lat, era5_lon from {{ source('silver', 'locations') }}
)
select r.site, r.year, r.month, r.rainfall_mm, r.chirps_lat, r.chirps_lon,
       h.temperature_c, h.soil_moisture, h.hours, c.era5_lat, c.era5_lon,
       cast(r.year * 100 + r.month as {{ 'string' if target.type == 'spark' else 'varchar' }}) as year_month
from {{ ref('stg_rainfall') }} r
join hourly h on r.site = h.site and r.year = h.year and r.month = h.month
join source_coordinates c on r.site = c.site
