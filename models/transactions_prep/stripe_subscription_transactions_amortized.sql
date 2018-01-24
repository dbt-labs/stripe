{{ config(
  materialized = "table",
  dist = "customer_id",
  sort = "transaction_date"
) }}

--this model rolls up the daily amortization of invoices to the last value for
--a customer in a given month

with transactions as (

    select * from {{ref('stripe_subscription_transactions_amortized_daily')}}

),

rollup as (

    select distinct

        last_value(source_item_type) over (partition by customer_id, date_month
            order by transaction_date rows between unbounded preceding and unbounded
            following) as source_item_type,
        last_value(source_item_id) over (partition by customer_id, date_month
            order by transaction_date rows between unbounded preceding and unbounded
            following) as source_item_id,
        last_value(subscription_id) over (partition by customer_id, date_month
            order by transaction_date rows between unbounded preceding and unbounded
            following) as subscription_id,
        customer_id,
        max(transaction_date) over (partition by customer_id, date_month
            order by transaction_date rows between unbounded preceding and unbounded
            following) as transaction_date,
        max(invoice_date) over (partition by customer_id, date_month
            order by transaction_date rows between unbounded preceding and unbounded
            following) as invoice_date,
        max(period_start) over (partition by customer_id, date_month
            order by transaction_date rows between unbounded preceding and unbounded
            following) as period_start,
        max(period_end) over (partition by customer_id, date_month
            order by transaction_date rows between unbounded preceding and unbounded
            following) as period_end,
        sum(case when customer_last_month_value = 1 then mrr_amount else null end)
            over (partition by customer_id, date_month order by transaction_date
            rows between unbounded preceding and unbounded following) as amount,
        last_value(plan_id) over (partition by customer_id, date_month
            order by transaction_date rows between unbounded preceding and unbounded
            following) as plan_id,
        last_value(forgiven) over (partition by customer_id, date_month
            order by transaction_date rows between unbounded preceding and unbounded
            following) as forgiven,
        last_value(paid) over (partition by customer_id, date_month
            order by transaction_date rows between unbounded preceding and unbounded
            following) as paid,
        last_value(duration) over (partition by customer_id, date_month
            order by transaction_date rows between unbounded preceding and unbounded
            following) as duration

    from transactions

)

select * from rollup
