select
  country_cd,
  year,
  piaac_cd,
  education_indicator_cd,
  education_indicator_value
from
  {{ ref('stg_education') }}
where
  age_cd = 'Y25T64'
  and gender_cd = 'T'
  and isceda_cd = '_T_T_T'