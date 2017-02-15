with events as (

  select * from {{ref('stripe_events')}}

)

select
  data__object__id as id,
  data__object__description as name,
  data__object__email as email,
  created as created_at,
  "type" as event_type
from events
where "type" in ('customer.deleted', 'customer.created', 'customer.updated')
