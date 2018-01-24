{{ config(
  materialized = "table",
  dist = "customer_id",
  sort = "transaction_date"
) }}

--this model amortizes values first into a daily output then summarizes by month

with transactions as (

    select * from {{ref('stripe_subscription_transactions_unioned')}}

),

days as (

    select * from {{ref('days')}}

),

amortized as (

    select

        transactions.*,

        dateadd(
            day,
            datediff(day, date_trunc('day', transactions.period_start),
            date_day),
            invoice_date
        ) as transaction_date

    from transactions

    inner join days
        on date_trunc('day', transactions.period_start) <= days.date_day
        and date_trunc('day', transactions.period_end) > days.date_day

),

month_days as (

    select
        *,
        split_part(last_day(date_trunc('month', transaction_date)), '-', 3)::float
            as month_days,
        split_part(transaction_date::date, '-', 3) as transaction_day,
        split_part(invoice_date::date, '-', 3) as invoice_day,
        date_trunc('month', transaction_date)::date as date_month
    from amortized

),

calculated as (

--the below fields calculated daily values for annual contracts to properly be
--able to sum up the amounts monthly regardless of the number of days in a month

    select
        *,
        1/month_days/12 as daily_calculated_proportion,
        (1/month_days/12)*amount as daily_calculated_amount,
        (1/month_days/12) * amount * month_days as calculated_mrr,
        case
            when max(transaction_day) over (partition by customer_id, date_month
            order by transaction_date rows between unbounded preceding and
            unbounded following) = transaction_day
                then 1
            else null
        end as customer_last_month_value,
        case
            when invoice_day = transaction_day
                then 1
            when invoice_day > month_days
            and transaction_day = month_days
                then 1
            else null
        end as rev_rec_date_base,
        case
            when max(date_month) over (partition by customer_id order by
            date_month rows between unbounded preceding and unbounded following)
            = date_month
                then 1
            else null
        end as last_month
    from month_days

),

final as (

    select
        *,
        case
            when last_month = 1
                then 0
            when duration = 'month'
                then amount
            else calculated_mrr
        end as mrr_amount
    from calculated

)

select * from final
