-- Reconcile each location and calendar month in both directions.
with source_counts as (
    select site, year, month, count(*) as hours
    from {{ ref('stg_climate_hourly') }}
    group by site, year, month
), gold_counts as (
    select site, year, month, hours
    from {{ ref('fact_climate_monthly') }}
)
select coalesce(s.site, g.site) as site,
       coalesce(s.year, g.year) as year,
       coalesce(s.month, g.month) as month
from source_counts s full outer join gold_counts g
    on s.site = g.site and s.year = g.year and s.month = g.month
where s.site is null or g.site is null or g.hours is null or s.hours <> g.hours
