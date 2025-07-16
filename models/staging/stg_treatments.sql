{{
    config(
        schema = 'stage',
        tags = 'treatments'
    )
}}

select 
    * 
from 
    {{ source('src_hospital', 'treatments') }}