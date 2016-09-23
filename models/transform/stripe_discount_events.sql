with events as (

  select * from {{ref('stripe_events')}}

), discount_events as (

  select
    data__object__id as id,
    created as created_at,
    "type" as event_type,
    data__object__customer as customer_id,
    data__object__coupon__id as coupon_id,
    case
      when data__object__coupon__percent_off is null
        then 'amount'
      else 'percent'
    end as discount_type,
    coalesce(data__object__coupon__percent_off, data__object__coupon__amount_off) as discount_value,
    data__object__start as discount_start,
    data__object__end as discount_end
  from events
  where "type" like 'customer.discount.%'

), d1 as (
  --need to add a field here to be used in a later query.
  select
    *,
    lag(discount_end, 1) over
      (partition by customer_id
       order by created_at
      ) as prior_discount_end
  from discount_events

)

--this is kinda shitty to have to do, but there are instances where there are duplicate records that are *exactly* the same
--but have different created_at's. this query de-dupes them.
select
  id,
  event_type,
  customer_id,
  coupon_id,
  discount_type,
  discount_value,
  discount_start,
  discount_end,
  min(created_at) as created_at
from d1
where event_type != 'customer.discount.deleted'
group by 1, 2, 3, 4, 5, 6, 7, 8

union all

--need to union together discount deleted entries because their date fields don't really support the logic we need
--to do in subsequent amortization queries. we need the discount to look cancelled for the entire remaining duration
--of the original discount.
select
  id,
  event_type,
  customer_id,
  coupon_id,
  discount_type,
  0::numeric(38,6) as discount_value,
  discount_end as discount_start,
  prior_discount_end as discount_end,
  created_at
from d1
where event_type = 'customer.discount.deleted'
