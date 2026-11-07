select 1 as incomplete_coverage
where (select count(*) from {{ ref('fact_climate_monthly') }}) <>
      (select count(*) from {{ ref('dim_location') }}) * (select count(*) from {{ ref('dim_month') }})
