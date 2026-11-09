-- Every audited climate year must have twelve calendar months and both providers.
with audit as (
    select year,
           sum(case when dataset = 'CHIRPS v3' then 1 else 0 end) as chirps_files,
           sum(case when dataset = 'ERA5-Land' then 1 else 0 end) as era5_files
    from {{ ref('source_audit') }}
    where dataset in ('CHIRPS v3', 'ERA5-Land')
    group by year
), calendar as (
    select year, count(distinct month) as months,
           min(month) as first_month, max(month) as last_month
    from {{ ref('dim_month') }}
    group by year
)
select coalesce(a.year, c.year) as year
from audit a full outer join calendar c on a.year = c.year
where a.year is null or c.year is null
   or a.chirps_files <> 1 or a.era5_files <> 1
   or c.months <> 12 or c.first_month <> 1 or c.last_month <> 12
