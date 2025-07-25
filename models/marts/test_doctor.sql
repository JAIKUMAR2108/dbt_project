{{ config(
    materialized='incremental',
    unique_key='doctor_id',
    post_hook=[ctrl_log_model1()],
    schema='test_dimension'
) }}

{% set last_run_time = get_last_run_time(this.name) %}

with source_data as (

    select *
    from {{ source('src_hospital', 'doctors') }}
    {% if is_incremental() %}
        where created_at > to_timestamp('{{ last_run_time }}')
    {% endif %}

),

final as (

    select * from source_data

)

select * from final
