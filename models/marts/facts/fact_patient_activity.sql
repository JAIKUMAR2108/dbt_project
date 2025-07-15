with a as(select
    patient.sk_patient as patient_key,
    count(*) as total_appointment,
    min(appointment.appointment_date) as first_appointment_date,
    max(appointment.appointment_date) as last_appointment_date,
    sum(case 
        when appointment.status='No-show' then 1 
        else 0
    end )as no_show_count  
from    
    {{ ref('dim_patients') }} patient 

left join
    {{ ref('fact_appointment') }} appointment
on  
    appointment.patient_key=patient.sk_patient
group by 
    patient.sk_patient
order by 
    patient.sk_patient
),

b as (
select
    patient_key,
    sum(amount) as total_billing,
    sum(
        CASE    
            when payment_status='Paid' then amount
            else 0
        end
    ) as total_paid 
from    
    {{ ref('fact_billing') }}
group by
    patient_key
)

select
    a.patient_key,
    a.total_appointment,
    b.total_billing,
    b.total_paid,
    a.no_show_count,
    a.first_appointment_date,
    a.last_appointment_date
from
    a a
left join 
    b b
on 
    a.patient_key=b.patient_key