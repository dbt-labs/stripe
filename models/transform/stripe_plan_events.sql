with events as (

  select * from {{ref('stripe_events')}}

)

select
  data__object__id as id,
  data__object__amount as amount,
  data__object__currency as currency,
  data__object__name as name,
  "type" as event_type,
  data__object__interval as plan_interval,
  created as created_at
from events
where "type" like 'plan%'
