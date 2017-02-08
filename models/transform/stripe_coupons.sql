with events as (

  select * from {{ref('stripe_coupon_events')}}

)

select distinct
  events.id,
  first_value(case when event_type != 'coupon.deleted' then created_at end) over
    (partition by events.id order by created_at rows between unbounded preceding and unbounded following)
    as created_at,
  last_value(currency) over
    (partition by events.id order by created_at rows between unbounded preceding and unbounded following)
    as currency,
  last_value(percent_discount) over
    (partition by events.id order by created_at rows between unbounded preceding and unbounded following)
    as percent_discount,
  last_value(duration) over
    (partition by events.id order by created_at rows between unbounded preceding and unbounded following)
    as duration,
  last_value(duration_in_months) over
    (partition by events.id order by created_at rows between unbounded preceding and unbounded following)
    as duration_in_months,
  last_value(max_redemptions) over
    (partition by events.id order by created_at rows between unbounded preceding and unbounded following)
    as max_redemptions,
  last_value(redeem_by) over
    (partition by events.id order by created_at rows between unbounded preceding and unbounded following)
    as redeem_by,
  last_value(amount_discount) over
    (partition by events.id order by created_at rows between unbounded preceding and unbounded following)
    as amount_discount,
  last_value(valid) over
    (partition by events.id order by created_at rows between unbounded preceding and unbounded following)
    as valid,
  last_value(case when event_type = 'coupon.deleted' then created_at end ignore nulls) over
    (partition by events.id order by created_at rows between unbounded preceding and unbounded following)
    as deleted_at
from events
