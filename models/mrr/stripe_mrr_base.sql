{% set window_clause = "partition by date_month, customer_id order by date_month rows between unbounded preceding and unbounded following" %}

with transactions as (

    select *
    from {{ref('stripe_transactions')}}
    where forgiven = false
        or forgiven is null

),

metrics as (

    select distinct

        date_month,
        customer_id,

        first_value(transaction_date)
            over ( {{window_clause}} )
            as rev_rec_date,

        last_value(plan_id ignore nulls)
            over ( {{ window_clause}} )
            as plan_id,

        min(period_start)
            over ( {{ window_clause}} )
            as period_start,

        max(period_end)
            over ( {{ window_clause}} )
            as period_end,

        sum(amount)
            over ( {{ window_clause}} )
            as mrr,

        sum(case
            when source_item_type in ('subscription payment', 'proration item')
                and date_trunc('month', period_start) = date_month
                then amount
            end)
            over ( {{ window_clause}} )
            as subscription_amount,

        sum(case
            when source_item_type in ('subscription payment', 'proration item')
                and date_trunc('month', period_start) != date_month
                then amount
            end)
            over ( {{ window_clause}} )
            as accrual_amount,

        sum(case when source_item_type = 'addon' then amount end)
            over ( {{ window_clause}} )
            as addon_amount

    from transactions

)

select *
from metrics
