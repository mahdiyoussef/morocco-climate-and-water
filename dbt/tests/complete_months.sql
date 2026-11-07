select * from {{ ref('fact_climate_monthly') }}
where hours <> day(last_day(make_date(year, month, 1))) * 24
