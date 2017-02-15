with charges as (

  select * from {{ref('stripe_events')}}

)

select
  id,
  data__object__customer as customer_id,
  data__object__amount as amount,
  trim('charge.' from "type") as result,
  data__object__created as created_at
from charges
where "type" like 'charge.%'
