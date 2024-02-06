select
  *
from
  {{ source('inequality', 'INCOMEDISTRO') }}