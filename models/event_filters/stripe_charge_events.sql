with charges as (

  select * from {{ref('stripe_events')}}

)

select
  id as event_id,
  data__object__id as charge_id,
  data__object__customer as customer_id,
  data__object__invoice as invoice_id,
  data__object__balance_transaction as balance_transaction_id,
  data__object__amount as amount,
  replace("type", 'charge.', '') as result,
  data__object__created as created_at
from charges
where "type" like 'charge.%'
