with incremented as (

    select * from {{ref('stripe_subscription_transactions_incremented')}}

),

final as (

    select

        source_item_type,
        source_item_id,
        subscription_id,
        customer_id,
        invoice_date,
        period_start,

        case
            when duration = 'year'
                then period_end

            when max_period_start <= date_part(day, period_end) then period_end

            else

                least(

                    to_date((
                        date_part(month, period_end)::varchar || '/' ||
                        date_part(day, last_day(period_end))::varchar || '/' ||
                        date_part(year, period_end)::varchar
                    ), 'mm/dd/yyyy'),

                    following_period_start

                )

        end as period_end,

        amount,
        plan_id,
        forgiven,
        paid,
        duration

    from incremented

)

select * from final
