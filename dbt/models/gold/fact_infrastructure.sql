with assets as (
  select site,
    sum(case when category = 'healthcare' then 1 else 0 end) as healthcare,
    sum(case when category = 'education' then 1 else 0 end) as education,
    sum(case when category = 'water' then 1 else 0 end) as water
  from {{ source('silver', 'osm_services') }} group by site
)
select l.id as site, coalesce(a.healthcare, 0) as healthcare,
       coalesce(a.education, 0) as education, coalesce(a.water, 0) as water,
       coalesce(r.road_km, 0) as road_km
from {{ ref('dim_location') }} l
left join assets a on l.id = a.site
left join {{ source('silver', 'osm_roads') }} r on l.id = r.site
