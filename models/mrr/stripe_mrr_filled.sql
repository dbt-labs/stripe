with mrr as (

    select * from {{ref('stripe_mrr_base')}}

),

months as (

    select * from {{ref('months')}}

),

customers as (

    select
        customer_id,
        min(date_month) as date_month_start,
        max(date_month) as date_month_end
    from mrr
    group by 1

),

customer_months as (

    select

        customers.customer_id,
        months.date_month

    from customers

    inner join months
        on customers.date_month_start <= months.date_month
        and customers.date_month_end >= months.date_month

),

joined as (

    select

      customer_months.date_month,
      customer_months.customer_id,
      mrr.rev_rec_date,
      mrr.plan_id,
      mrr.period_start,
      mrr.period_end,
      mrr.mrr,
      mrr.subscription_amount,
      mrr.accrual_amount,
      mrr.addon_amount,
      --this is to fill in the revenue recognition date for rows that have no invoices
      case when mrr.rev_rec_date is null then
          last_value(mrr.rev_rec_date ignore nulls) over (
              partition by customer_months.customer_id
              order by customer_months.date_month
              rows between unbounded preceding and current row
          )
      end as last_good_rev_rec_date

    from customer_months

    left outer join mrr
        on customer_months.customer_id = mrr.customer_id
        and customer_months.date_month = mrr.date_month

),

final as (

    select

        date_month,
        customer_id,
        --this is to fill in the revenue recognition date for rows that have no invoices
        case
            when rev_rec_date is not null then rev_rec_date
            else dateadd(
                month,
                datediff(
                    month,
                    date_trunc('month', last_good_rev_rec_date),
                    date_month
                ),
                last_good_rev_rec_date)
        end as rev_rec_date,

        plan_id,
        period_start,
        period_end,
        mrr,
        subscription_amount,
        accrual_amount,
        addon_amount

    from joined

)

select * from final
