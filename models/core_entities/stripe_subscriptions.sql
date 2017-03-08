{% set partition_clause = "partition by id, period_start order by created_at rows between unbounded preceding and unbounded following" %}

with events as (

    select * from {{ref('stripe_subscription_events')}}

),

final as (

    select distinct

        id,

        last_value(event_id) over ( {{ partition_clause }} ) as event_id,
        last_value(customer_id) over ( {{ partition_clause }} ) as customer_id,

        last_value(created_at) over ( {{ partition_clause }} ) as created_at,

        last_value(status) over ( {{ partition_clause }} ) as status,
        last_value(event_type) over ( {{ partition_clause }} ) as event_type,

        last_value(start) over ( {{ partition_clause }} ) as start,
        last_value(period_start) over ( {{ partition_clause }} ) as period_start,
        last_value(period_end) over ( {{ partition_clause }} ) as period_end,
        last_value(canceled_at) over ( {{ partition_clause}} ) as canceled_at,

        last_value(quantity) over ( {{ partition_clause }} ) as quantity,

        last_value(plan_id) over ( {{ partition_clause }} ) as plan_id,
        last_value(plan_interval) over ( {{ partition_clause }} ) as plan_interval,
        last_value(plan_amount) over ( {{ partition_clause }} ) as plan_amount

    from events

)

select * from final
