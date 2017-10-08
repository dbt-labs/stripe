with transactions as (

    select * from {{ref('stripe_subscription_transactions')}}

),

prorations as (

    select * from {{ref('stripe_proration_transactions')}}

),

months as (

    select * from {{ref('months')}}

),

unioned as (

    select * from transactions
    union all
    select * from prorations

),

amortized as (

    select

        unioned.*,

        dateadd(
            month,
            datediff(month, date_trunc('month', unioned.period_start), date_month),
            invoice_date
        ) as transaction_date


    from unioned

    inner join months
        on date_trunc('month', unioned.period_start) <= months.date_month
        and date_trunc('month', unioned.period_end) > months.date_month

),

final as (

    select

        source_item_type,
        source_item_id,
        subscription_id,
        customer_id,
        transaction_date,
        invoice_date,
        period_start,
        period_end,

        case
            when duration = 'month' then amount
            when duration = 'year' then amount /
                datediff(month, period_start, period_end)
        end as amount,

        plan_id,
        forgiven,
        paid,
        duration

    from amortized


)

select * from final
