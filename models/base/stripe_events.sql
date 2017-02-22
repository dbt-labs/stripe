select *
from {{ var('events_table') }}
where livemode = true
