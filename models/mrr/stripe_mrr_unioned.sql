select * from {{ref('stripe_mrr_xf')}}

union all

select * from {{ref('stripe_mrr_final_churn')}}
