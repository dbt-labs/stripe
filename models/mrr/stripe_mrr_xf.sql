with mrr as (

    select * from {{ref('stripe_mrr_filled')}}

),

plans as (

    select * from {{ref('stripe_plans')}}

),

joined as (

    select

        md5(date_month::varchar || customer_id) as id,
        date_month,
        customer_id,
        rev_rec_date,
        coalesce(plan_id, 'unknown') as plan_id,
        period_start,
        period_end,
        coalesce(mrr, 0) as mrr,
        coalesce(subscription_amount, 0) as subscription_amount,
        coalesce(accrual_amount, 0) as accrual_amount,
        coalesce(addon_amount, 0) as addon_amount,
        coalesce(plans.name, 'unknown') as plan_name,
        plan_mrr_amount::float / 100 as plan_mrr_amount,
        plans.plan_interval

    from mrr

    left outer join plans on mrr.plan_id = plans.id

),

final as (

    select

        *,

        case when mrr > 0 then 1 else 0 end as active_customer,

        case
        when first_value(case when mrr > 0 then date_month end) over (
                partition by customer_id
                order by date_month
                rows between unbounded preceding and unbounded following
            ) = date_month
            then 1
        else 0
        end as first_month,

        case
        when last_value(case when mrr > 0 then date_month end) over (
                partition by customer_id
                order by date_month
                rows between unbounded preceding and unbounded following
            ) = date_month
            and period_end < current_date
            then 1
        else 0
        end as last_month

    from joined

)

select * from final
