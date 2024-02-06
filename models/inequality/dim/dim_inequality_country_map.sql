select
  location as country_cd,
  country
from
  {{ ref('raw_income_distribution') }}
group by
  1, 2