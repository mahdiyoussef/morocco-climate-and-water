select site, year, month from {{ ref('fact_climate_monthly') }} group by site, year, month having count(*) <> 1
