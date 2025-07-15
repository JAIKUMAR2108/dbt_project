{{
    config(
        materialized='table',
        schema='fact'
    )
}}

select
    concat(appointments.sk_appointment,patients.sk_patient,doctors.sk_doctor) as appointment_key,
    patients.patient_id as patient_key,
    doctors.doctor_id as doctor_key,
    appointments.appointment_date,
    appointments.appointment_time,
    appointments.reason_for_visit,
    appointments.status
from 
    {{ ref('dim_patients') }} patients join {{ ref('int_appointments') }} appointments
on  
    patients.patient_id=appointments.patient_id
join    
    {{ ref('dim_doctor') }} doctors
on  
    doctors.doctor_id=appointments.doctor_id
