select *
from {{ var('events_table') }}
where data__object__livemode = true
