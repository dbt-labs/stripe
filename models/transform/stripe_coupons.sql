with events as (

  select * from {{ref('stripe_events')}}

), coupon_events as (

  select
    created as created_at,
    data__object__currency as currency,
    data__object__id as id,
    data__object__percent_off as percent_discount,
    data__object__duration as duration,
    data__object__duration_in_months as duration_in_months,
    data__object__max_redemptions as max_redemptions,
    data__object__redeem_by as redeem_by,
    data__object__amount_off as amount_discount,
    data__object__valid as valid,
    "type" as event_type
  from events
  where "type" like 'coupon.%'

), deleted_coupons as (

  select id, created_at as deleted_at
  from coupon_events
  where event_type = 'coupon.deleted'

)

select distinct
  coupon_events.id,
  first_value(created_at) over
    (partition by coupon_events.id order by created_at rows between unbounded preceding and unbounded following)
    as created_at,
  last_value(currency) over
    (partition by coupon_events.id order by created_at rows between unbounded preceding and unbounded following)
    as currency,
  last_value(percent_discount) over
    (partition by coupon_events.id order by created_at rows between unbounded preceding and unbounded following)
    as percent_discount,
  last_value(duration) over
    (partition by coupon_events.id order by created_at rows between unbounded preceding and unbounded following)
    as duration,
  last_value(duration_in_months) over
    (partition by coupon_events.id order by created_at rows between unbounded preceding and unbounded following)
    as duration_in_months,
  last_value(max_redemptions) over
    (partition by coupon_events.id order by created_at rows between unbounded preceding and unbounded following)
    as max_redemptions,
  last_value(redeem_by) over
    (partition by coupon_events.id order by created_at rows between unbounded preceding and unbounded following)
    as redeem_by,
  last_value(amount_discount) over
    (partition by coupon_events.id order by created_at rows between unbounded preceding and unbounded following)
    as amount_discount,
  last_value(valid) over
    (partition by coupon_events.id order by created_at rows between unbounded preceding and unbounded following)
    as valid,
  deleted_coupons.deleted_at
from coupon_events
  left outer join deleted_coupons on coupon_events.id = deleted_coupons.id
where event_type != 'coupon.deleted'
