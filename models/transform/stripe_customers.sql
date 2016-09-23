with events as (

  select * from {{ref('stripe_customer_events')}}

), deleted_customers as (

  select id, created_at as deleted_at
  from events
  where event_type = 'customer.deleted'

)

select distinct
  events.id,
  last_value(name) over
    (partition by events.id order by created_at rows between unbounded preceding and unbounded following)
    as name,
  last_value(email) over
    (partition by events.id order by created_at rows between unbounded preceding and unbounded following)
    as email,
  first_value(created_at) over
    (partition by events.id order by created_at rows between unbounded preceding and unbounded following)
    as created_at,
  deleted_customers.deleted_at
from events
  left outer join deleted_customers on events.id = deleted_customers.id
where events.event_type in ('customer.created', 'customer.updated')
