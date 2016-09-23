with events as (

  select * from {{ref('stripe_events')}}

), d1 as (

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

)

select
  id,
  last_value(amount) over
  (partition by id
   order by created_at
   rows between unbounded preceding and unbounded following
  ) as amount,
  last_value(currency) over
  (partition by id
   order by created_at
   rows between unbounded preceding and unbounded following
  ) as currency,
  last_value(name) over
  (partition by id
   order by created_at
   rows between unbounded preceding and unbounded following
  ) as name,
  last_value(plan_interval) over
  (partition by id
   order by created_at
   rows between unbounded preceding and unbounded following
  ) as plan_interval,
  first_value(created_at) over
  (partition by id
   order by created_at
   rows between unbounded preceding and unbounded following
  ) as created_at,
  last_value(created_at) over
  (partition by id
   order by created_at
   rows between unbounded preceding and unbounded following
  ) as updated_at
from d1
where event_type != 'plan.deleted'
