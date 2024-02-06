select
  c.country,
  year,
  i.indicator,
  inequality_indicator_value as indicator_value,
  rank() over (partition by f.country_cd, inequality_indicator_cd order by inequality_indicator_value desc) as rank
from
  {{ ref('fact_income_distribution_full_population') }} f
join
  {{ ref('dim_inequality_country_map') }} c
  on c.country_cd = f.country_cd
join
  {{ ref('dim_inequality_indicator') }} i
  on i.indicator_cd = f.inequality_indicator_cd
order by
  country, rank