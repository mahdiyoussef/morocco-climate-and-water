select year, month,
       cast(year * 100 + month as {{ 'string' if target.type == 'spark' else 'varchar' }}) as year_month,
       sum(rainfall_mm * cos(radians(lat))) / sum(cos(radians(lat))) as area_weighted_rainfall_mm,
       count(*) as valid_grid_cells,
       sum(6371.0 * 6371.0 * radians(0.05) * (sin(radians(lat + 0.025)) - sin(radians(lat - 0.025)))) as approximate_coverage_km2
from {{ source('silver', 'chirps_grid') }}
group by year, month
