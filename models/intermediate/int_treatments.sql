{{
    config(
        schema = 'INTERMEDIATE'
    )
}}


select 
    concat('t', row_number() over (order by treatment_id)) as sk_treatment, 
    treatment_id,
    coalesce(appointment_id,'unknown') as appointment_id,
    coalesce(treatment_type,'unknown') as treatment_type,
    coalesce(description,'unknown') as description,
    coalesce(cost,0) as cost,
    coalesce(treatment_date,'01-01-1900') as treatment_date
from 
    {{ ref('stg_treatments') }}