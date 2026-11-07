select site, observed_at, temperature_c, soil_moisture,
       cast(year(observed_at) as int) as year,
       cast(month(observed_at) as int) as month
from {{ source('silver', 'era5_hourly') }}
