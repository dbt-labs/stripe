with dates as (

    select * from {{ref('days')}}

), final as (

    select distinct
        date_trunc('month', date_day)::date as date_month
    from dates

)

select * from final
