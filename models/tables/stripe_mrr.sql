{{ config(materialized = "view") }}

select *
from {{ref('stripe_mrr_churned')}}
