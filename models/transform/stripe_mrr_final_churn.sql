with mrr as (

  select * from {{ref('stripe_mrr_discounted')}}

)

select
  subscription_event_id,
  dateadd(day, 1, date_day)::date as date_day,
  customer_id,
  0::numeric(38,6) as mrr,
  plan_interval,
  null::boolean as first_day,
  null::boolean as last_day
from mrr
where last_day = true
  and date_day < current_date
