select
  country_cd,
  country
from
  {{ ref('raw_education') }}
group by
  1, 2