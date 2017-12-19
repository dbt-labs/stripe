with joined as (

    select * from {{ref('stripe_subscription_transactions_joined')}}

),

incremented as (

    --adjust transaction dates to prepare for amortization
    --subscription payments are for the subsequent month, not the prior month.
    --the first month always shows zero duration, so correct for that as well.

    select

        source_item_type,
        source_item_id,
        subscription_id,
        customer_id,
        invoice_date,

        case
            when period_end - period_start = 0 then period_start
            else period_end
        end as period_start,

        case
            when duration = 'month' then dateadd(month, 1, period_end)
            when duration = 'year' then dateadd(year, 1, period_end)
        end as period_end,

        amount,
        plan_id,
        forgiven,
        paid,
        duration,

        case
            when row_number() over (partition by customer_id order by invoice_date) = 1
            and duration = 'year'
            and amount = 0
                then 1
            else 0
        end as annual_flag

    from joined

),

final as (

    select

        *,

        max(date_part(day, period_start)) over (
            partition by subscription_id
            ) as max_period_start,

        lead(period_start, 1) over (
            partition by subscription_id order by invoice_date
        ) as following_period_start

    from incremented
    where annual_flag = 0

)

select * from final
