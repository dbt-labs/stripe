with mrr as (

  select * from {{ref('stripe_mrr')}}

), days as (

  select
    date_day,
    date_trunc('month', date_day)::date as date_month,
    count(*) as customers,
    sum(mrr) as mrr,
    sum(mrr) * 12 as arr,
    sum(mrr) / count(*) as arpa
  from mrr
  where active_customer = true
  group by 1, 2

), last_values as (

  select distinct
    date_month,
    last_value(customers) over (partition by date_month order by date_day rows between unbounded preceding and unbounded following) as customers,
    last_value(mrr) over (partition by date_month order by date_day rows between unbounded preceding and unbounded following) as mrr
  from days

), grouped_values as (

  select
    date_trunc('month', date_day)::date as date_month,
    coalesce(sum(case when change_category = 'new' then mrr_change end), 0) as new_mrr,
    coalesce(sum(case when change_category = 'upgrade' then mrr_change end), 0) as upgrade_mrr,
    coalesce(sum(case when change_category = 'churn' then mrr_change end), 0) as churned_mrr,
    coalesce(sum(case when change_category = 'reactivation' then mrr_change end), 0) as reactivation_mrr,
    coalesce(sum(case when change_category = 'downgrade' then mrr_change end), 0) as downgrade_mrr,
    coalesce(sum(case when change_category = 'new' then 1 end), 0) as new_customers,
    coalesce(sum(case when change_category = 'upgrade' then 1 end), 0) as upgrade_customers,
    coalesce(sum(case when change_category = 'churn' then 1 end), 0) as churned_customers,
    coalesce(sum(case when change_category = 'reactivation' then 1 end), 0) as reactivation_customers,
    coalesce(sum(case when change_category = 'downgrade' then 1 end), 0) as downgrade_customers
  from mrr
  group by 1

), joined as (

  select
    last_values.date_month,
    customers,
    mrr,
    lag(mrr) over (order by last_values.date_month) as beginning_mrr,
    new_mrr,
    upgrade_mrr,
    churned_mrr,
    reactivation_mrr,
    downgrade_mrr,
    new_customers,
    upgrade_customers,
    churned_customers,
    reactivation_customers,
    downgrade_customers,
    new_mrr + reactivation_mrr as total_new_mrr,
    new_mrr + upgrade_mrr + churned_mrr + reactivation_mrr + downgrade_mrr as net_change_in_mrr,
    new_customers - churned_customers + reactivation_customers as net_change_in_customers,
    upgrade_mrr + downgrade_mrr as net_upgrade_mrr
  from last_values
    left outer join grouped_values on last_values.date_month = grouped_values.date_month

)

select
  *,
  churned_mrr::float / beginning_mrr * -1 as gross_churn_rate,
  (churned_mrr + upgrade_mrr + downgrade_mrr)::float / beginning_mrr * -1 as net_churn_rate,
  mrr::float / customers as arpa,
  new_mrr::float / nullif(new_customers, 0) as new_customer_arpa,
  churned_mrr::float / nullif(churned_customers, 0) * -1 as churned_customer_arpa,
  upgrade_mrr::float / nullif(upgrade_customers, 0) as avg_upgrade,
  downgrade_mrr::float / nullif(downgrade_customers, 0) * -1 as avg_downgrade,
  net_upgrade_mrr::float / beginning_mrr as net_upgrade_rate
from joined
where date_month < date_trunc('month', current_date)
order by 1
