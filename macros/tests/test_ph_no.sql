{% test ph_no(model, column_name) %}

select 
    {{ column_name }}
from 
    {{ model }}
where
    length(cast({{ column_name }} as string)) != 10
{% endtest %}

