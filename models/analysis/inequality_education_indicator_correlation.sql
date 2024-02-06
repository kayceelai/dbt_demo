with analyze as (
  select
    i.country_cd,
    i.year,
    i.inequality_indicator_cd,
    i.inequality_indicator_value,
    e.education_indicator_cd,
    e.piaac_cd,
    e.education_indicator_value
  from
    {{ ref('fact_income_distribution_full_population') }} i
  inner join 
  {{ ref('fact_education_full_population') }} e
    on i.country_cd = e.country_cd and i.year = e.year
)
-- select * from analyze
select
  c.country,
  a.year,
  i1.indicator as inequality_indicator,
  i2.indicator as education_indicator,
  p.category as piaac_category,
  cast(
    corr(a.inequality_indicator_value, a.education_indicator_value) as decimal(18, 5)
  ) as correlation
from
  analyze a
left join
  {{ ref('dim_country') }} c
  on c.country_cd = a.country_cd
left join
  {{ ref('dim_inequality_indicator') }} i1
  on i1.indicator_cd = a.inequality_indicator_cd
left join
  {{ ref('dim_education_indicator') }} i2
  on i2.indicator_cd = a.education_indicator_cd
left join
  {{ ref('dim_piaac_category') }} p
  on p.category_cd = a.piaac_cd
group by
  c.country,
  a.year,
  i1.indicator,
  i2.indicator,
  p.category