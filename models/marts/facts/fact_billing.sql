{{
    config(
        schema ='fact'
    )
}}

select
    treatment.appointment_id as appointment_key,
    patient.sk_patient as patient_key,
    dim_treatment_type.sk_treatment as treatment_key,
    billing.bill_date as bill_date,
    billing.amount as amount,
    billing.payment_method as payment_method,
    billing.payment_status as payment_status 
from
    {{ ref('dim_patients') }} patient join {{ ref('int_billing') }} billing 
on  
    patient.patient_id=billing.patient_id
join 
    {{ ref('int_treatments') }} treatment
on      
    treatment.treatment_id=billing.treatment_id
join
    {{ ref('dim_treatment_type') }} dim_treatment_type
on  
    treatment.treatment_type=dim_treatment_type.treatment_type
and 
    treatment.description=dim_treatment_type.description
