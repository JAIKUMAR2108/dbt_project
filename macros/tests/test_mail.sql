{% test patient_mail(model, column_name) %}

select 
    {{ column_name }}
from 
    {{ model }}
where 
    lower({{column_name}}) not like '%mail.com' 
{% endtest %}

{% test doctor_mail(model, column_name)%}

select
    {{column_name}}
from    
    {{model}}
where
    lower({{column_name}}) not like '%@hospital.com'

{% endtest %}