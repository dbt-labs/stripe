with events as (

    select * from {{ref('stripe_events')}}

)

select

    data__object__id as id,
    nullif(data__object__subscription, '') as subscription_id,
    nullif(data__object__charge, '') as charge_id,
    nullif(data__object__customer, '') as customer_id,
    nullif(id, '') as event_id,

    "type" as event_type,

    data__object__date as invoice_date,
    data__object__period_end as period_end,
    data__object__period_start as period_start,

    data__object__currency as currency,
    data__object__attempt_count as attempt_count,
    data__object__attempted as attempted,
    data__object__closed as closed,

    data__object__total as total,
    data__object__subtotal as subtotal,

    data__object__amount_due as amount_due,
    data__object__next_payment_attempt as next_payment_attempt,
    data__object__paid as paid,
    data__object__forgiven as forgiven,

    created as created_at

from events
where "type" like 'invoice.%'
    and "type" not like '%payment%'
