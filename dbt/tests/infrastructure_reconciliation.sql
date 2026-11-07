with counts as (
  select site, count(*) as objects from {{ source('silver', 'osm_services') }} group by site
)
select f.site from {{ ref('fact_infrastructure') }} f
left join counts c on f.site = c.site
where f.water + f.healthcare + f.education <> coalesce(c.objects, 0)
