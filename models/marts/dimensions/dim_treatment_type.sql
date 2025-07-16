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
        concat(cast(min(cost)as string),' - ',cast(max(cost) as string)) as cost_range,
        current_timestamp() as last_updated
    from    
        {{ ref('int_treatments') }}
    group by 
    treatment_type,
    description
)

select * from min_max






