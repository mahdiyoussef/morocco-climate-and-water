-- Detect loss of hourly observations during monthly aggregation.
with source_counts as (
  select site, count(*) as hours from {{ source('silver', 'era5_hourly') }} group by site
), gold_counts as (
  select site, sum(hours) as hours from {{ ref('fact_climate_monthly') }} group by site
)
select s.site from source_counts s left join gold_counts g on s.site = g.site
where g.site is null or s.hours <> g.hours
