with mrr as (

    select * from {{ref('stripe_mrr_xf')}}

),

thin_air as (

    select

        dateadd(month, 1, date_month)::date as date_month,
        customer_id,
        dateadd(month, 1, rev_rec_date) as rev_rec_date,
        plan_id,
        null::timestamp as period_start,
        null::timestamp as period_end,
        0::float as mrr,
        0::float as subscription_amount,
        0::float as accrual_amount,
        0::float as addon_amount,
        plan_name,
        plan_mrr_amount,
        plan_interval,
        0::int as active_customer,
        0::int as first_month,
        0::int as last_month


    from mrr

    where last_month = 1

),

final as (

    select

        md5(date_month::varchar || customer_id) as id,
        *

    from thin_air

)

select * from final
