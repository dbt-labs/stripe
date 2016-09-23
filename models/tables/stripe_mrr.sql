{{ config(

  materialized = "table",
  dist = "customer_id",
  sort = "date_day"

) }}

select *
from {{ref('stripe_mrr_churned')}}
