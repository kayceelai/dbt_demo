select
  measure_cd as indicator_cd,
  measure as indicator
from
  {{ ref('raw_income_distribution') }}
group by
  1, 2