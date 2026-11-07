select id, name, lat, lon, zone from {{ source('silver', 'locations') }}
