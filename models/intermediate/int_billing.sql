{{
    config(
        schema = 'INTERMEDIATE',
        tags = 'billing'
    )
}}

select 
    concat('b', row_number() over (order by bill_id)) AS sk_bill,
    coalesce(bill_id,'unknown') as bill_id,
    coalesce(patient_id,'unknown') as patient_id,
    coalesce(treatment_id,'unknown') as treatment_id,
    coalesce(bill_date,'01-01-1900') as bill_date,
    coalesce(amount,0) as amount,
    coalesce(payment_method,'unknown') as payment_method,
    coalesce(payment_status,'unknown') as payment_status
from 
    {{ ref('stg_billing') }}