with events as (

  select * from {{ref('stripe_events')}}

), all_the_days as (

  select (min(created) over () + row_number() over ())::date as date_day
  from events

)

select *
from all_the_days
where date_day <= convert_timezone('{{ var('timezone') }}', current_date)
