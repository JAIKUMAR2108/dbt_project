{{
    config(
        schema = 'stage',
        tags = 'patients'
    )
}}

select 
    * 
from 
    {{ source('src_hospital', 'patients') }}