{{
    config(
        materialized ='incremental',
        unique_key ='treatment_id',
        strategy ='merge',
        schema = 'dimensions'
    )
}}

select 
    treatment_type,
    description,
    concat(cast(min(cost) as string),' - ',cast(max(cost) as string)) as cost_range,
    current_timestamp as last_update
from
    {{ ref("int_treatments") }}
GROUP BY 
    treatment_type,
    description

