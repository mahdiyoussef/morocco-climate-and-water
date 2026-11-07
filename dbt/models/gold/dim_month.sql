select distinct
  cast(year * 100 + month as {{ 'string' if target.type == 'spark' else 'varchar' }}) as year_month,
  year, month,
  case month when 1 then 'Jan' when 2 then 'Feb' when 3 then 'Mar' when 4 then 'Apr'
    when 5 then 'May' when 6 then 'Jun' when 7 then 'Jul' when 8 then 'Aug'
    when 9 then 'Sep' when 10 then 'Oct' when 11 then 'Nov' when 12 then 'Dec' end as month_name,
  make_date(year, month, 1) as month_start
from {{ source('silver', 'chirps_grid') }}
