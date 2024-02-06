select
  location as country_cd,
  measure_cd as inequality_indicator_cd,
  age_cd,
  year,
  value as inequality_indicator_value
from
  {{ ref('raw_income_distribution') }}