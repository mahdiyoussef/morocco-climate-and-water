select site, year, month, rainfall_mm, chirps_lat, chirps_lon
from {{ source('silver', 'chirps_sites') }}
