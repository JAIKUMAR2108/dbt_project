{% macro log_model_test(model_name,run_id,run_started_at,run_ended_at, control_table='HOSPITAL_DATA_MODEL.CONTROL_TABLE.CT') %}

{% set run_duration=(run_ended_at.timestamp() - run_started_at.timestamp()) %}

{% set status='success' if execute else 'failed' %}

insert into {{control_table}}(model_name,run_id,run_started_at,run_ended_at,status,run_duration_sec) values
(
    '{{model_name}}',
    '{{run_id}}',
    '{{run_started_at}}',
    '{{run_ended_at}}',
    '{{status}}',
     {{run_duration}}
);

{% endmacro %}


{% macro run_started_at_test() %}

{% set run_started_at =modules.datetime.datetime.utcnow()  %}
{% do return(run_started_at) %}

{% endmacro %}



