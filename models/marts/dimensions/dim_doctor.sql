{{
    config(
        materialized='incremental',
        strategy='merge',
        unique_key='doctor_id',
        schema='dimensions',
        tags='doctors',
        merge_exclude_columns=['last_updated_date'] 
    )
}}

with source as (
    select
        sk_doctor,
        doctor_id,
        first_name,
        last_name,
        specialization,
        phone_number,
        years_experience,
        hospital_branch,
        email,
        current_timestamp() as new_last_updated_date
    from {{ ref('int_doctors') }}
),

final as (
    select
        s.sk_doctor,
        s.doctor_id,
        s.first_name,
        s.last_name,
        s.specialization,
        s.phone_number,
        s.years_experience,
        s.hospital_branch,
        s.email,

        case
            when t.doctor_id is null  
              or s.first_name != t.first_name
              or s.last_name != t.last_name
              or s.specialization != t.specialization
              or s.phone_number != t.phone_number
              or s.years_experience != t.years_experience
              or s.hospital_branch != t.hospital_branch
              or s.email != t.email
            then s.new_last_updated_date
            else t.last_updated_date
        end as last_updated_date
    from source s
    left join {{ this }} t
      on s.doctor_id = t.doctor_id
)

select * from final
