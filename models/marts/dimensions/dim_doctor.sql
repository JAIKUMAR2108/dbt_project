{{
    config(
        materialized ='incremental',
        strategy ='merge',
        unique_key ='doctor_id',
        schema ='dimensions'
    )
}}

select
    *
from
    {{ ref('int_doctors') }}
