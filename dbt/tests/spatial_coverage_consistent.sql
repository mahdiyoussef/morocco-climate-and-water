select year, month from {{ ref('mart_spatial_rainfall') }}
where valid_grid_cells <> (select max(valid_grid_cells) from {{ ref('mart_spatial_rainfall') }})
   or approximate_coverage_km2 <= 0
