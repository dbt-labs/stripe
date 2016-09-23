with mrr as (

  select * from {{ref('stripe_mrr_discounted')}}

), churn as (

  select * from {{ref('stripe_mrr_final_churn')}}

), all_mrr as (

  select * from mrr
  union all
  select * from churn

), mrr_with_changes as (

  -- Use lag() to supplement the customer MRR data with deltas
  -- relative to the proceeding row.
  select
    *,
    lag(mrr) over (partition by customer_id order by date_day)
      as prior_mrr,
    mrr - coalesce(lag(mrr) over
      (partition by customer_id order by date_day), 0)
      as mrr_change
  from all_mrr

)

select
  date_day,
  customer_id,
  mrr,
  prior_mrr,
  mrr_change,
  plan_interval,
  case
    when first_day is true and mrr > 0
      then 'new'
    -- A downgrade to mrr = 0 is considered a "churn"
    when mrr = 0 and prior_mrr > 0
      then 'churn'
    -- An upgrade from mrr = 0 is considered a "reactivation"
    when mrr_change > 0 and prior_mrr = 0
      then 'reactivation'
    when mrr_change > 0
      then 'upgrade'
    when mrr_change < 0
      then 'downgrade'
    else
      null
  end as change_category,
  case
    when mrr > 0 then true
    else false
  end as active_customer
from mrr_with_changes
