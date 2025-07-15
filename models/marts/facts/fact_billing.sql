{{
    config(
        schema ='fact'
    )
}}

select
    concat(billing.sk_bill,patient.sk_patient,treatment.sk_treatment) as appointment_key,
    patient.sk_patient as patient_key,
    treatment.sk_treatment as treatment_key,
    billing.bill_date as bill_date,
    billing.amount as amount,
    billing.payment_method as payment_method,
    billing.payment_status as payment_status 
from
    {{ ref('dim_patients') }} patient join {{ ref('int_billing') }} billing 
on  
    patient.patient_id=billing.patient_id
join 
    {{ ref('dim_treatment_type') }} treatment
on      
    treatment.treatment_id=billing.treatment_id