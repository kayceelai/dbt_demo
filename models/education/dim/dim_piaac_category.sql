select
  piaac_category as category_cd,
  category
from
  {{ ref('raw_education') }}
group by
  1, 2