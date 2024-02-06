select
  c.country,
  year,
  p.category as piaac_category,
  i.indicator,
  education_indicator_value as indicator_value,
  rank() over (partition by f.country_cd, education_indicator_cd order by education_indicator_value desc) as rank
from
  {{ ref('fact_education_full_population') }} f
join
  {{ ref('dim_education_country_map') }} c
  on c.country_cd = f.country_cd
join
  {{ ref('dim_education_indicator') }} i
  on i.indicator_cd = f.education_indicator_cd
join
  {{ ref('dim_piaac_category') }} p
  on p.category_cd = f.piaac_cd
order by
  country, rank