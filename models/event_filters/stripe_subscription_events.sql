with events as (

    select * from {{ref('stripe_events')}}

),

final as (

    select

        data__object__id as id,
        id as event_id,
        data__object__customer as customer_id,

        created as created_at,

        data__object__status as status,
        "type" as event_type,

        data__object__start as start,
        data__object__current_period_start as period_start,
        data__object__current_period_end as period_end,
        data__object__canceled_at as canceled_at,

        data__object__quantity as quantity,

        data__object__plan__id as plan_id,
        data__object__plan__interval as plan_interval,
        data__object__plan__amount as plan_amount

    from events

    where "type" like 'customer.subscription.%'

)

select * from final
