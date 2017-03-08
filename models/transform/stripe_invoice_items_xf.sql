with discounts as (

    select * from {{ref('stripe_discounts')}}

),

invoice_items as (

    select * from {{ref('stripe_invoice_items')}}

),

joined as (

    select

        invoice_items.*,

        case
            when discounts.discount_type = 'percent'
                then amount * (1.0 - discounts.discount_value::float / 100)
            else amount - discounts.discount_value
        end as discounted_amount

    from invoice_items

    left outer join discounts
        on invoice_items.customer_id = discounts.customer_id
        and invoice_items.invoice_date > discounts.discount_start
        and (invoice_items.invoice_date < discounts.discount_end
             or discounts.discount_end is null)

),

final as (

    select

        id,
        invoice_id,
        customer_id,
        event_id,
        subscription_id,
        invoice_date,
        period_start,
        period_end,
        proration,
        plan_id,
        amount,
        coalesce(discounted_amount, amount) as discounted_amount,
        currency,
        description,
        created_at,
        deleted_at

    from joined

)

select * from final
