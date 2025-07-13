{% snapshot snapshot_name %}
    {{
        config(
            target_schema='HOSPITAL_DATA_MODEL.INTERMEDIATE',
            target_database='HOSPITAL_DATA_MODEL',
            unique_key='patient_id',
            strategy='timestamp',
        )
    }}

    select 
    patient_id,
    first_name,
    last_name,
    gender,
    data_of_birth,
    contact_number,
    address,
    insurance_provider,
    insurance_number,
    email,
    registration_date
     from {{ source('src_hospital', 'patients') }}
 {% endsnapshot %}