{{
    config(
        schema = 'stage',
        tags = 'billing'
        
        
    )
}}

select 
    * 
from 
    {{ source('src_hospital', 'billing') }}