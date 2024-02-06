with countries as (
select
  country_cd,
  country
from
  {{ ref('dim_education_country_map') }}
union
select
  country_cd,
  country
from
  {{ ref('dim_inequality_country_map') }}
)
select
  country_cd,
  replace(country, '''') as country
from
  countries
group by
  1, 2