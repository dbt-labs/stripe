with transactions as (

    select * from {{ref('stripe_mrr')}}

),

core_metrics as (

    select

        date_month,
        sum(active_customer) as customers,
        sum(mrr) as mrr,
        sum(subscription_amount) as subscription_amount,
        sum(accrual_amount) as accrual_amount,
        sum(addon_amount) as addon_amount,

        coalesce(sum(
            case when change_category = 'new' then mrr_change end
            ), 0) as new_mrr,

        coalesce(
            sum(case when change_category = 'upgrade' then mrr_change end
            ), 0) as upgrade_mrr,

        coalesce(sum(
            case when change_category = 'churn' then mrr_change end
            ), 0) as churned_mrr,

        coalesce(sum(
            case when change_category = 'reactivation' then mrr_change end
            ), 0) as reactivation_mrr,

        coalesce(sum(
            case when change_category = 'downgrade' then mrr_change end
            ), 0) as downgrade_mrr,

        coalesce(sum(
            case when change_category = 'new' then 1 end
            ), 0) as new_customers,

        coalesce(sum(
            case when change_category = 'upgrade' then 1 end
            ), 0) as upgrade_customers,

        coalesce(sum(
            case when change_category = 'churn' then 1 end
            ), 0) as churned_customers,

        coalesce(sum(
            case when change_category = 'reactivation' then 1 end
            ), 0) as reactivation_customers,

        coalesce(sum(
            case when change_category = 'downgrade' then 1 end
            ), 0) as downgrade_customers

    from transactions
    group by 1

),

lagged as (

    select
        *,
        lag(mrr) over (order by date_month) as beginning_mrr
    from core_metrics

),

composite as (

    select

        *,

        new_mrr + reactivation_mrr as total_new_mrr,

        new_mrr + upgrade_mrr + churned_mrr + reactivation_mrr + downgrade_mrr
            as net_change_in_mrr,

        new_customers - churned_customers + reactivation_customers
            as net_change_in_customers,

        upgrade_mrr + downgrade_mrr as net_upgrade_mrr

    from lagged

),

ratios as (

    select

        *,

        churned_mrr::float / nullif(beginning_mrr, 0) * -1 as gross_churn_rate,

        (churned_mrr + upgrade_mrr + downgrade_mrr)::float /
            nullif(beginning_mrr, 0) * -1 as net_churn_rate,

        mrr::float / nullif(customers, 0) as arpa,
        new_mrr::float / nullif(new_customers, 0) as new_customer_arpa,

        churned_mrr::float / nullif(churned_customers, 0) * -1
            as churned_customer_arpa,

        upgrade_mrr::float / nullif(upgrade_customers, 0) as avg_upgrade,

        downgrade_mrr::float / nullif(downgrade_customers, 0) * -1
            as avg_downgrade,

        net_upgrade_mrr::float / nullif(beginning_mrr, 0) as net_upgrade_rate

    from composite

)

select *
from ratios
where date_month < date_trunc('month', current_date)
order by 1
