{{
    config(
        materialized ='incremental',
        strategy ='merge',
        unique_key ='doctor_id',
        schema ='dimensions',
        tags = 'doctors'
    )
}}

select
    *
from
    {{ ref('int_doctors') }}
