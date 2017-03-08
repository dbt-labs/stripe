{% set partition_clause = "partition by id order by created_at rows between unbounded preceding and unbounded following" %}

with events as (

    select * from {{ref('stripe_discount_events')}}

)

select distinct
    id,

    last_value(customer_id) over ( {{ partition_clause }} ) as customer_id,
    last_value(coupon_id) over ( {{ partition_clause }} ) as coupon_id,
    last_value(discount_type) over ( {{ partition_clause }} ) as discount_type,
    last_value(discount_value) over ( {{ partition_clause }} ) as discount_value,
    last_value(discount_start) over ( {{ partition_clause }} ) as discount_start,
    last_value(discount_end) over ( {{ partition_clause }} ) as discount_end,

    first_value(created_at) over ( {{ partition_clause }} ) as created_at,

    min(
        case when event_type = 'customer.discount.deleted'
        then created_at
        end
    ) over ( {{ partition_clause }} ) as deleted_at

from events
