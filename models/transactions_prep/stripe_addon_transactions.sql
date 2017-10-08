with items as (

    select * from {{ref('stripe_invoice_items_xf')}}

),

invoices as (

    select * from {{ref('stripe_invoices')}}

),

final as (

    select

        'addon' as source_item_type,
        items.id as source_item_id,
        items.subscription_id,
        items.customer_id,
        items.invoice_date as transaction_date,
        items.invoice_date,
        items.period_start,
        items.period_end,
        items.discounted_amount as amount,
        items.plan_id,
        invoices.forgiven,
        invoices.paid,
        'one-time' as duration

    from items

    inner join invoices on items.invoice_id = invoices.id

    where items.proration = false
        and items.deleted_at is null

)

select * from final
