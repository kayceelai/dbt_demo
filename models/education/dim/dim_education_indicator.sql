select
  indicator_cd as indicator_cd,
  indicator as indicator
from
  {{ ref('raw_education') }}
group by
  1, 2