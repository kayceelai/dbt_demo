select
  *
from {{ source('education', 'EAG_EA_SKILLS') }}