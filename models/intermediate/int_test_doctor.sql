{{
    config(
        schema='test_intermediate'
    )
}}

select * from {{ ref('stg_test_doctor') }}