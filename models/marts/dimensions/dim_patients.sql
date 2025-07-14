{{
    config(
        materialized='table',
        schema ='dimensions'
    )
}}

select 
    concat('d', ROW_NUMBER() OVER (ORDER BY dbt_valid_from)) AS surrogate_key,
    patient_id,
    first_name,
    last_name,
    gender,
    date_of_birth,
    contact_number,
    address,
    insurance_provider,
    insurance_number,
    email,
    registration_date,
    dbt_valid_from as effective_start_date,
    dbt_valid_to as effective_end_date,
    CASE
        when effective_end_date is null then 'Y'
        else 'N'
    end as is_current
from 
    {{ ref('int_patient_snapshot') }}