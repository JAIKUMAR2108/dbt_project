{% set run_started_at = run_started_at_test() %}
{{
    config(
        materialized='incremental',
        unique_key='doctor_id',
        schema='test_dimension',
        post_hook=["{{ log_model_test(this.identifier,invocation_id,run_started_at,modules.datetime.datetime.utcnow()) }}"]
    )
}}

select * from {{ ref('int_test_doctor') }}