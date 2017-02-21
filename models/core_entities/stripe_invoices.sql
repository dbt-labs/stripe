{% set partition_clause = "partition by id order by created_at rows between unbounded preceding and unbounded following" %}

with events as (

    select * from {{ref('stripe_invoice_events')}}

), consolidated as (

    select distinct

        id,

        last_value(subscription_id) over ( {{ partition_clause }} ) as subscription_id,
        last_value(charge_id) over ( {{ partition_clause }} ) as charge_id,
        last_value(customer_id) over ( {{ partition_clause }} ) as customer_id,
        last_value(event_id) over ( {{ partition_clause }} ) as event_id,
        last_value(invoice_date) over ( {{ partition_clause }} ) as invoice_date,
        last_value(period_start) over ( {{ partition_clause }} ) as period_start,
        last_value(period_end) over ( {{ partition_clause }} ) as period_end,
        last_value(currency) over ( {{ partition_clause }} ) as currency,
        last_value(attempt_count) over ( {{ partition_clause }} ) as attempt_count,
        last_value(attempted) over ( {{ partition_clause }} ) as attempted,
        last_value(closed) over ( {{ partition_clause }} ) as closed,
        last_value(total) over ( {{ partition_clause }} ) as total,
        last_value(subtotal) over ( {{ partition_clause }} ) as subtotal,
        last_value(amount_due) over ( {{ partition_clause }} ) as amount_due,
        last_value(next_payment_attempt) over ( {{ partition_clause }} ) as next_payment_attempt,
        last_value(paid) over ( {{ partition_clause }} ) as paid,
        last_value(forgiven) over ( {{ partition_clause }} ) as forgiven,

        first_value(created_at) over ( {{ partition_clause }} ) as created_at

    from events

), final as (

    select

        id,
        subscription_id,
        charge_id,
        customer_id,
        event_id,
        invoice_date,
        period_start,
        period_end,
        currency,
        attempt_count,
        attempted,
        closed,
        total,
        subtotal,
        amount_due,
        next_payment_attempt,
        paid,
        --sometimes forgiven is null but it's clear that it shouldn't be
        --and we can infer the value (we can't always infer successfully)
        --this only happens to records prior to 2015.
        case
            when forgiven is not null then forgiven
            else
                case when paid = true then false end
        end as forgiven,

        created_at,
        total - amount_due as amount_paid

    from consolidated

)

select * from final
