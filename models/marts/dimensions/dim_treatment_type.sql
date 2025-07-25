{{
    config(
        materialized ='incremental',
        unique_key ='sk_treatment',
        strategy ='merge',
        schema = 'dimensions',
        tags = 'treatments'
    )
}}

with min_max as (
    select 

        concat('tt', row_number() over (order by treatment_type)) AS sk_treatment,
        treatment_type,
        description,
        concat(cast(min(cost)as string),' - ',cast(max(cost) as string)) as cost_range
    from    
        {{ ref('int_treatments') }}
    group by 
    treatment_type,
    description
)


{% if is_incremental() %}

, target as(
select 
    *
from        
    {{this}}
)

select
    src.sk_treatment,
    src.treatment_type,
    src.description,
    src.cost_range,
    coalesce(tgt.created_at,current_timestamp()) as created_at,
    current_timestamp() as  updated_at
from    
    min_max src left join target tgt
on
    src.sk_treatment=tgt.sk_treatment
where   
    tgt.sk_treatment is null or
    src.treatment_type <> tgt.treatment_type or
    src.description <> tgt.description or
    src.cost_range <> tgt.cost_range


{% else %}

select 
*,
current_timestamp() as created_at,
current_timestamp() as updated_at
from 
min_max


{% endif %}





