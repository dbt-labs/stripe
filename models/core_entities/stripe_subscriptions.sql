with events as (

  select * from {{ref('stripe_subscription_events')}}

)

select
  id,
  event_id,
  customer_id,
  created_at,
  -- this  is to handle mid-period upgrades, which the stripe subscription records don't give you
  -- a lot of clue what is going on. you need to determine if this start date is the same as the previous start date and
  -- infer from that whether there is something new going on.
  case status
    when 'active' then
      case start
        when lag(start, 1) over (partition by customer_id order by created_at)
          then period_start
        else start
      end
    when 'canceled' then
      case
        when prior_status = 'past_due'
          then period_start
        else created_at
      end
    else period_start
  end as period_start,
  period_end,
  event_type,
  status,
  prior_status,
  plan_interval,
  plan_id,
  quantity,
  case status
    -- Special case: The events for cancellations have positive
    -- period_amount values so we need to treat them as zeros
    when 'canceled'
      then 0
    else
      -- Normalize annual billing to monthly using simple division
      case plan_interval
        when 'year'
          then coalesce(period_amount, 0)::numeric(38,6) / 12 / 100
        else coalesce(period_amount, 0)::numeric(38,6) / 100
      end
  end as mrr
from events
