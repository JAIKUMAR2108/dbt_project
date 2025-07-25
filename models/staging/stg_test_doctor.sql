{{
    config(
        schema='test_stg'
    )
}}

select * from {{ source('src_hospital','doctors') }}