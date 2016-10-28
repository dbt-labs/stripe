with events as (

  select * from {{ref('stripe_events')}}

)

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
