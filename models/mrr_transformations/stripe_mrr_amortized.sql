with events as (

  select * from {{ref('stripe_subscriptions')}}

), days as (

  select * from {{ref('days')}}

), events_filtered as (
  --there are a bunch of extra records that we don't need that actually end up clogging the periods.
  --this cte filters those extra records out and deduplicates the remaining ones.
  --duplicates are injected when there are overdue accounts--this causes multiple events for the same period.
  select distinct
    customer_id,
    period_start::date,
    period_end::date,
    last_value(event_type) over (
      partition by customer_id, period_start::date, period_end::date
      order by created_at
      rows between unbounded preceding and unbounded following
    ) as event_type,
    last_value(status) over (
      partition by customer_id, period_start::date, period_end::date
      order by created_at
      rows between unbounded preceding and unbounded following
    ) as status,
    last_value(mrr) over (
      partition by customer_id, period_start::date, period_end::date
      order by created_at
      rows between unbounded preceding and unbounded following
    ) as mrr,
    last_value(plan_interval) over (
      partition by customer_id, period_start::date, period_end::date
      order by created_at
      rows between unbounded preceding and unbounded following
    ) as plan_interval,
    last_value(id) over (
      partition by customer_id, period_start::date, period_end::date
      order by created_at
      rows between unbounded preceding and unbounded following
    ) as id,
    last_value(created_at) over (
      partition by customer_id, period_start::date, period_end::date
      order by created_at
      rows between unbounded preceding and unbounded following
    ) as created_at
  from events
  where status not in ('trialing', 'past_due')
    and (event_type != 'customer.subscription.deleted' or prior_status = 'past_due')

), customers as (
  -- this CTE grabs the begin and end date for a given customer; we need this to create days records for the entire duration.
  select
    customer_id,
    min(period_start) as customer_start,
    max(period_end) as customer_end
  from events_filtered
  group by 1

), customer_days as (
  -- one record for every day of the customer's lifetime (even if they were inactive in the middle)
  select customer_id, date_day
  from customers
    inner join days
      on customers.customer_start <= days.date_day
      and customers.customer_end > days.date_day

)

select distinct
  customer_days.customer_id,
  customer_days.date_day,
  coalesce(last_value(mrr) over
    (partition by customer_days.customer_id, customer_days.date_day
     order by created_at
     rows between unbounded preceding and unbounded following
    ), 0) as mrr,
  last_value(events_filtered.id) over
    (partition by customer_days.customer_id, customer_days.date_day
     order by created_at
     rows between unbounded preceding and unbounded following
   ) as subscription_event_id,
  last_value(plan_interval) over
     (partition by customer_days.customer_id, customer_days.date_day
      order by created_at
      rows between unbounded preceding and unbounded following
    ) as plan_interval
from customer_days
  left outer join events_filtered
    on customer_days.date_day >= events_filtered.period_start
    and customer_days.date_day < events_filtered.period_end
    and customer_days.customer_id = events_filtered.customer_id
