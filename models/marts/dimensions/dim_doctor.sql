{{
    config(
        materialized='incremental',
        strategy='merge',
        unique_key='doctor_id',
        schema='dimensions',
        tags='doctors',
        merge_exclude_columns=['created_at', 'last_updated_date']
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
        current_timestamp() as etl_time
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

        -- Created at: only set when row is new
        case
            when t.doctor_id is null then s.etl_time
            else t.created_at
        end as created_at,

        -- Last updated: set when data changes
        case
            when t.doctor_id is null
              or s.first_name IS DISTINCT FROM t.first_name
              or s.last_name IS DISTINCT FROM t.last_name
              or s.specialization IS DISTINCT FROM t.specialization
              or s.phone_number IS DISTINCT FROM t.phone_number
              or s.years_experience IS DISTINCT FROM t.years_experience
              or s.hospital_branch IS DISTINCT FROM t.hospital_branch
              or s.email IS DISTINCT FROM t.email
            then s.etl_time
            else t.last_updated_date
        end as last_updated_date
    from source s
    left join {{ this }} t
        on s.doctor_id = t.doctor_id
)

select * from final
