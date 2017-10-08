{{
    config(
        materialized = 'table',
        sort = 'date_month',
        dist = 'customer_id'
    )
}}

with mrr as (

    select * from {{ref('stripe_mrr_unioned')}}

),


mrr_with_changes as (

    select

        *,

        coalesce(
            lag(mrr) over (partition by customer_id order by date_month),
            0) as prior_mrr,

        mrr - coalesce(
            lag(mrr) over (partition by customer_id order by date_month),
            0
            ) as mrr_change

    from mrr

),

final as (

    select

        *,

        case
            when first_month = 1 and mrr > 0 then 'new'
            when active_customer = 0
                and lag(active_customer)
                over (partition by customer_id order by date_month) = 1
                then 'churn'
            when lag(active_customer)
                over (partition by customer_id order by date_month) = 0
                and active_customer = 1
                then 'reactivation'
            when mrr_change > 0 then 'upgrade'
            when mrr_change < 0 then 'downgrade'
        end as change_category,

        least(mrr, prior_mrr) as renewal_amount

    from mrr_with_changes

)

select * from final
