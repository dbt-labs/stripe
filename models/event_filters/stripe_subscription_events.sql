with events as (

  select * from {{ref('stripe_events')}}

)

select
  id,
  created as created_at,
  data__object__customer as customer_id,
  data__object__plan__interval as plan_interval,
  data__object__quantity as quantity,
  data__object__plan__amount * coalesce(data__object__quantity, 1) as period_amount,
  data__object__current_period_start as period_start,
  data__object__current_period_end as period_end,
  data__object__start as start,
  data__object__status as status,
  lag(status, 1) over (partition by customer_id order by created_at) as prior_status,
  "type" as event_type,
  data__object__plan__id as plan_id
from events
where "type" like 'customer.subscription.%'
