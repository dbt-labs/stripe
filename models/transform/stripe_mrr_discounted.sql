with mrr as (

  select * from {{ref('stripe_mrr_amortized')}}

), discounts as (

  select * from {{ref('stripe_discounts')}}

), d1 as (

  select
    mrr.subscription_event_id,
    mrr.date_day,
    mrr.customer_id,
    case discount_type
      when 'percent' then
        mrr * (1 - (discount_value / 100.0))
      when 'amount' then
        case
        -- cents to dollars
        when mrr > (discount_value / 100.0) then
          round(mrr - (discount_value / 100.0), 2)
        else
          0
        end
      else
        mrr
    end as mrr,
    plan_interval
  from mrr
    left outer join discounts
      on mrr.date_day = discounts.date_day
      and mrr.customer_id = discounts.customer_id

)

select
  *,
  case min(date_day) over(partition by customer_id)
    when date_day then true
    else false
    end as first_day,
  case max(date_day) over(partition by customer_id)
    when date_day then true
    else false
  end as last_day
from d1
