with mrr as (

  select * from {{ref('stripe_mrr_typed')}}

), churns as (

  select
    date_day,
    customer_id,
    mrr,
    prior_mrr,
    mrr_change,
    lag(plan_interval, 1) ignore nulls over
      (partition by customer_id order by date_day) as plan_interval,
    lag(product, 1) ignore nulls over
      (partition by customer_id order by date_day) as product,
    change_category,
    active_customer
  from mrr

)

select
  *
from churns
where change_category = 'churn'

union all

select
  *
from mrr
where change_category != 'churn' or change_category is null
