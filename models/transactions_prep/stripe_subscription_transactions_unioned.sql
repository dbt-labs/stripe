{{ config(
  materialized = "table",
  dist = "customer_id",
  sort = "invoice_date"
) }}

--this unions prorations from invoice items with the invoice data so you can
--view all transactions with dates prior to the data going through amortization

with transactions as (

    select * from {{ref('stripe_subscription_transactions')}}

),

prorations as (

    select * from {{ref('stripe_proration_transactions')}}

),

unioned as (

    select *
    from transactions

    union all

    select *
    from prorations

)

select * from unioned
