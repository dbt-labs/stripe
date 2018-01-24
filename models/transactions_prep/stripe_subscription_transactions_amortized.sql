{{ config(
  materialized = "table",
  dist = "customer_id",
  sort = "transaction_date"
) }}

{% set frame_clause = 'over (partition by customer_id, date_month
    order by transaction_date rows between unbounded preceding and unbounded
    following)'%}

--this model rolls up the daily amortization of invoices to the last value for
--a customer in a given month

with transactions as (

    select * from {{ref('stripe_subscription_transactions_amortized_daily')}}

),

rollup as (

    select distinct

        last_value(source_item_type) {{frame_clause}} as source_item_type,
        last_value(source_item_id) {{frame_clause}} as source_item_id,
        last_value(subscription_id) {{frame_clause}} as subscription_id,
        customer_id,
        min(case when rev_rec_date_base = 1 then transaction_date else null end)
            {{frame_clause}} as transaction_date,
        max(invoice_date) {{frame_clause}} as invoice_date,
        max(period_start) {{frame_clause}} as period_start,
        max(period_end) {{frame_clause}} as period_end,
        sum(case when customer_last_month_value = 1 then mrr_amount else null end)
            over (partition by customer_id, date_month order by transaction_date
            rows between unbounded preceding and unbounded following) as amount,
        last_value(plan_id) {{frame_clause}} as plan_id,
        last_value(forgiven) {{frame_clause}} as forgiven,
        last_value(paid) {{frame_clause}} as paid,
        last_value(duration) {{frame_clause}} as duration

    from transactions

)

select * from rollup
