with invoices as (

    select * from {{ref('stripe_invoices')}}

),

items as (

    select * from {{ref('stripe_invoice_items_xf')}}

),

subscriptions as (

    select * from {{ref('stripe_subscriptions')}}

),

amount_from_items as (

    --find the amount of addons for this invoice to subtract later

    select
        invoice_id,
        sum(discounted_amount) as amount
    from items
    where deleted_at is null
    group by 1

),

joined as (

    select

        'subscription payment'::varchar as source_item_type,
        invoices.id as source_item_id,
        invoices.subscription_id,
        invoices.customer_id,
        invoices.invoice_date,
        invoices.period_start,
        invoices.period_end,
        invoices.total - coalesce(amount_from_items.amount, 0) as amount,
        subscriptions.plan_id,
        invoices.forgiven,
        invoices.paid,
        case
            when plan_interval is not null
                then plan_interval
            when datediff(month, invoices.period_start, invoices.period_end) > 1
                then 'year'
            else 'month'
        end as duration

    from invoices

    left outer join subscriptions
        on invoices.subscription_id = subscriptions.id
        and invoices.period_start = subscriptions.period_start

    left outer join amount_from_items
        on invoices.id = amount_from_items.invoice_id

    where invoices.subscription_id is not null

)

select * from joined
