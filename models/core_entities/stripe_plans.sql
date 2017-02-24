with events as (

    select * from {{ref('stripe_plan_events')}}

),

core as (

    select distinct

        id,

        last_value(amount) over
            (partition by id
            order by created_at
            rows between unbounded preceding and unbounded following
            ) as amount,

        last_value(currency) over
            (partition by id
            order by created_at
            rows between unbounded preceding and unbounded following
            ) as currency,

        last_value(name) over
            (partition by id
            order by created_at
            rows between unbounded preceding and unbounded following
            ) as name,

        last_value(plan_interval) over
            (partition by id
            order by created_at
            rows between unbounded preceding and unbounded following
            ) as plan_interval,

        first_value(created_at) over
            (partition by id
            order by created_at
            rows between unbounded preceding and unbounded following
            ) as created_at,

        last_value(created_at) over
            (partition by id
            order by created_at
            rows between unbounded preceding and unbounded following
            ) as updated_at

    from events

    where event_type != 'plan.deleted'

),

final as (

    select

        *,

        case
            when plan_interval = 'month' then amount
            when plan_interval = 'year' then amount::float / 12
        end as plan_mrr_amount

    from core

)

select * from final
