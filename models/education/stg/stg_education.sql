select
  country_cd,
  isc11a as isceda_cd,
  sex as gender_cd,
  age_cd,
  piaac_category as piaac_cd,
  indicator_cd as education_indicator_cd,
  year,
  value as education_indicator_value
from
  {{ ref('raw_education') }}