with discount_events as (

  select * from {{ref('stripe_discount_events')}}

), days as (

  select * from {{ref('days')}}

)

select distinct
  customer_id,
  date_day,
  last_value(coupon_id) over
    (partition by customer_id, date_day
      order by created_at
      rows between unbounded preceding and unbounded following
    ) as coupon_id,
  last_value(discount_type) over
    (partition by customer_id, date_day
      order by created_at
      rows between unbounded preceding and unbounded following
    ) as discount_type,
  last_value(discount_value) over
    (partition by customer_id, date_day
      order by created_at
      rows between unbounded preceding and unbounded following
    ) as discount_value,
  first_value(created_at) over
    (partition by customer_id, date_day
      order by created_at
      rows between unbounded preceding and unbounded following
    ) as created_at
from discount_events
  inner join days
    on discount_events.discount_start::date <= days.date_day
    and (discount_events.discount_end::date > days.date_day or discount_events.discount_end is null)
