{{
    config(
        schema = 'stage',
        tags = 'appointments'
    )
}}

select 
    * 
from 
    {{ source('src_hospital', 'appointments') }}