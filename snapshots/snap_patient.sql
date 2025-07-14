{% snapshot int_patient_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='patient_id',
        strategy='check',
        check_cols=['sk_patient','first_name','last_name','gender','date_of_birth','contact_number','address','registration_date','insurance_provider','insurance_number','email']
    )
}}

select
    *
from    
    {{ ref('int_patients') }}

{% endsnapshot %}