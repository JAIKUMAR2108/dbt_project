{{
    config(
        schema ='dimensions',
        materialized ='table'
    )
}}

select
    doctor_id,
    first_name,
    last_name,
    specialization,
    phone_number,
    years_experience,
    hospital_branch,
    email,
    last_updated_date
from
    {{ ref('int_doctors') }}