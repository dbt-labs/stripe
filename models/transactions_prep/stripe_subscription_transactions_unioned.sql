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

),

ordered as (

    select
        *,
        dense_rank() over (partition by customer_id order by period_start)
            as customer_start_rank
    from unioned
    order by period_start, amount

),

calculate as (

    select
        *,
        date_diff('month', period_start, period_end) as diff,
        case
            when source_item_type = 'proration item'
            and duration = 'year'
                then last_value(case when amount > 0
                    and duration = 'year' then amount else null end ignore nulls)
                    over (partition by customer_id order by customer_start_rank, amount
                    rows between unbounded preceding and 1 preceding)
            else null
        end as prior_period_amount,
        case
            when source_item_type = 'proration item'
            and duration = 'year'
                then last_value(case when amount > 0
                    and duration = 'year' then period_start else null end ignore nulls)
                    over (partition by customer_id order by customer_start_rank, amount
                    rows between unbounded preceding and 1 preceding)
            else null
        end as prior_period_start,
        case
            when source_item_type = 'proration item'
            and duration = 'year'
                then last_value(case when amount > 0
                    and duration = 'year' then period_end else null end ignore nulls)
                    over (partition by customer_id order by customer_start_rank, amount
                    rows between unbounded preceding and 1 preceding)
            else null
        end as prior_period_end
    from ordered

)

select * from calculate
