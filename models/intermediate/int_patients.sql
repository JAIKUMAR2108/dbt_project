{{
    config(
        schema = 'INTERMEDIATE'
    )
}}

select
    concat('p', row_number() over (order by patient_id)) as sk_patient, 
    patient_id,
    coalesce(first_name,'unknown') as first_name,
    coalesce(last_name,'unknown') as last_name,
    coalesce(gender,'unknown') as gender,
    coalesce(date_of_birth,'01-01-1900') as date_of_birth,
    case 
        when LENGTH(contact_number) = 10 then contact_number
        else 0
    end as contact_number,
    coalesce(address,'unknown') as address,
    coalesce(registration_date,'01-01-1900') as registration_date,
    coalesce(insurance_provider,'unknown') as insurance_provider,
    coalesce(insurance_number,'unknown') as insurance_number,
    case 
        when email ilike '%@mail.com' then email
        else 'invalid mail id'
    end as email
from 
    {{ ref('stg_patients') }}