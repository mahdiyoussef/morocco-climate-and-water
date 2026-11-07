-- Annual/partial water-year grouping must conserve the full rainfall sum.
with monthly as (
  select site, sum(rainfall_mm) as mm from {{ ref('fact_climate_monthly') }} group by site
), yearly as (
  select site, sum(rainfall_mm) as mm from {{ ref('mart_water_year') }} group by site
)
select m.site from monthly m left join yearly y on m.site = y.site
where y.site is null or abs(m.mm - y.mm) > 0.00001
