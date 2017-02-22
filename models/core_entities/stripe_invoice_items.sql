{% set partition_clause = "partition by id order by created_at rows between unbounded preceding and unbounded following" %}

with events as (

    select * from {{ref('stripe_invoice_item_events')}}

)

select distinct
    id,

    last_value(invoice_id) over ( {{ partition_clause }} ) as invoice_id,
    last_value(customer_id) over ( {{ partition_clause }} ) as customer_id,
    last_value(event_id) over ( {{ partition_clause }} ) as event_id,
    last_value(invoice_date) over ( {{ partition_clause }} ) as invoice_date,
    last_value(period_start) over ( {{ partition_clause }} ) as period_start,
    last_value(period_end) over ( {{ partition_clause }} ) as period_end,
    last_value(amount) over ( {{ partition_clause }} ) as amount,
    last_value(currency) over ( {{ partition_clause }} ) as currency,
    last_value(description) over ( {{ partition_clause }} ) as description,

    first_value(created_at) over ( {{ partition_clause }} ) as created_at,

    min(
        case when event_type = 'invoiceitem.deleted'
        then created_at
        end
    ) over ( {{ partition_clause }} ) as deleted_at

from events
