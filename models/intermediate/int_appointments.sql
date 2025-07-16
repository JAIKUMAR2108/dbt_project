{{
    config(
        schema = 'INTERMEDIATE',
        tags = 'appointments'
    )
}}

select 
    concat('a', row_number() over (order by appointment_id)) AS sk_appointment,
    coalesce(appointment_id,'unknown') as appointment_id,
    coalesce(patient_id,'unknown') as patient_id,
    coalesce(doctor_id,'unknown') as doctor_id,
    coalesce(appointment_date,'01-01-1900') as appointment_date,
    coalesce(appointment_time,'00:00:00') as appointment_time,
    coalesce(reason_for_visit,'unknown') as reason_for_visit,
    coalesce(status,'unknown') as status
from
    {{ ref('stg_appointments') }}
