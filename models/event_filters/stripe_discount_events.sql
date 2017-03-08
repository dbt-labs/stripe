with events as (

    select * from {{ref('stripe_events')}}

),

final as (

    select

        data__object__id as id,
        created as created_at,
        "type" as event_type,
        data__object__customer as customer_id,
        data__object__coupon__id as coupon_id,

        case
            when data__object__coupon__percent_off is null then 'amount'
            else 'percent'
        end as discount_type,

        coalesce(data__object__coupon__percent_off,
                data__object__coupon__amount_off) as discount_value,

        data__object__start as discount_start,
        data__object__end as discount_end

    from events

    where "type" like 'customer.discount.%'

)

select * from final
