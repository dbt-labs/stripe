{{ config(
  materialized = "table",
  dist = "customer_id",
  sort = "transaction_date"
) }}

with transactions as (

    select * from {{ref('stripe_subscription_transactions_unioned')}}

),

months as (

    select * from {{ref('months')}}

),

amortized as (

    select

        transactions.*,

        dateadd(
            month,
            datediff(month, date_trunc('month', transactions.period_start),
            date_month),
            invoice_date
        ) as transaction_date


    from transactions

    inner join months
        on date_trunc('month', transactions.period_start) <= months.date_month
        and date_trunc('month', transactions.period_end) > months.date_month

    where diff > 0

),

combined as (

    select * from amortized

    union all

    select *, invoice_date as transaction_date
    from transactions
    where diff = 0

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

            --for first row of negative prorations for annual accounts, this
            --calculates the amount of $ already used
            when duration = 'year'
            and source_item_type = 'proration item'
            and amount < 0
            and row_number() over (partition by customer_id, source_item_id
                order by transaction_date) = 1

                then ((prior_period_amount /
                datediff('month', prior_period_start, prior_period_end)) -
                (prior_period_amount + amount)) *-1

            --for subsequent rows of negative annual prorations, it is important
            --to simply null out the remainder of the MRR
            when duration = 'year'
            and source_item_type = 'proration item'
            and amount < 0
            and row_number() over (partition by customer_id, source_item_id
                order by transaction_date) > 1
                then (prior_period_amount /
                    datediff('month', prior_period_start, prior_period_end)) * -1

            --for first row of positive prorations for annual accounts, this
            --calculates the amount for the first month

            when duration = 'year'
            and source_item_type = 'proration item'
            and amount > 0
            and row_number() over (partition by customer_id, source_item_id
                order by transaction_date) = 1
                then amount / datediff('second', period_start, period_end) --amount per second
                * datediff('second', period_start,
                to_date((
                        date_part(month,dateadd('month', 1, period_start))::varchar
                        || '/' ||
                        date_part(day, period_end)::varchar || '/' ||
                        date_part(year, period_start)::varchar
                    ), 'mm/dd/yyyy'))


            --for subsequent rows of positive annual prorations, it is important
            --to figure out what the annualized value would have been
            when duration = 'year'
            and source_item_type = 'proration item'
            and amount > 0
            and row_number() over (partition by customer_id, source_item_id
                order by transaction_date) > 1

                then amount / datediff('second', period_start, period_end) --amount per second
                * 31622400 / 12 --need to figure out leap year

            when duration = 'year' then amount /
                datediff(month, period_start, period_end)
        end as amount,

        plan_id,
        forgiven,
        paid,
        duration

    from combined


)

select * from final
