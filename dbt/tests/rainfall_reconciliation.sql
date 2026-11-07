-- Every valid site-month source total must reach Gold unchanged.
select s.site, s.year, s.month
from {{ source('silver', 'chirps_sites') }} s
left join {{ ref('fact_climate_monthly') }} f
  on s.site = f.site and s.year = f.year and s.month = f.month
where f.site is null or abs(s.rainfall_mm - f.rainfall_mm) > 0.000001
