{{
    config(
        schema = 'stage'
    )
}}

select 
    * 
from 
    {{ source('src_hospital', 'billing') }}