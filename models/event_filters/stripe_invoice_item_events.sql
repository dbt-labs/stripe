with events as (

    select * from {{var('events_table')}}

)

select

    data__object__id as invoice_item_id,
    data__object__invoice as invoice_id,
    data__object__customer as customer_id,
    id as event_id,

    "type" as event_type,

    data__object__date as invoice_date,
    data__object__period__start as period_start,
    data__object__period__end as period_end,

    data__object__amount as amount,
    data__object__currency as currency,

    data__object__description as description,

    created as created_at

from events
where "type" like 'invoiceitem.%'
    and livemode = true
order by created desc
