select
  country_cd,
  inequality_indicator_cd,
  year,
  inequality_indicator_value
from
  {{ ref('stg_income_distribution') }}
where
  age_cd = 'TOT'