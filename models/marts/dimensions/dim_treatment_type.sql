{{
    config(
        materialized ='incremental',
        unique_key ='treatment_id',
        strategy ='merge',
        schema = 'dimensions'
    )
}}

with min_max as (
    select 
        concat('tt', row_number() over (order by treatment_type)) AS sk_treatment,
        treatment_type,
        description,
        concat(cast(min(cost)as string),' - ',cast(max(cost) as string)) as cost_range,
        current_timestamp() as last_updated
    from    
        {{ ref('int_treatments') }}
    group by 
    treatment_type,
    description
)

select * from min_max

-- select 
--     treatment.sk_treatment,
--     treatment.treatment_id,
--     row_number() over (partition by treatment.treatment_type order by treatment.treatment_id) as treatment_type,
--     row_number() over (partition by treatment.description order by treatment.treatment_id) as description,
--     min_max.cost_range,
--     current_timestamp as last_update
-- from
--     {{ ref("int_treatments") }} treatment
-- join 
--     min_max min_max 
-- on  
--     treatment.treatment_type=min_max.treatment_type
-- and
--     treatment.description=min_max.description




