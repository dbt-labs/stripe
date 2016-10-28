with events as (

  select * from {{ref('stripe_coupon_events')}}

), deleted_coupons as (

  select id, created_at as deleted_at
  from events
  where event_type = 'coupon.deleted'

)

select distinct
  events.id,
  first_value(created_at) over
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
  deleted_coupons.deleted_at
from events
  left outer join deleted_coupons on events.id = deleted_coupons.id
where event_type != 'coupon.deleted'
