{% macro result_log_test(results,control_table='HOSPITAL_DATA_MODEL.CONTROL_TABLE.CT') %}

{% for result in results %}

{% set status=result.status %}
{% set model_name=result.node.name %}
{% set run_id=invocation_id %}
{% set run_started_at = run_started_at_test() %}
{% set run_ended_at=modules.datetime.datetime.utcnow() %}
{% set run_duration=(run_ended_at.timestamp()-run_started_at.timestamp()) %}

insert into {{control_table}}(model_name,run_id,run_started_at,run_ended_at,status,run_duration)
values(
    '{{model_name}}',
    '{{run_id}}',
    '{{run_started_at}}',
    '{{run_ended_at}}',
    '{{status}}',
    {{run_duration}}
);

{% endfor %}

{% endmacro %}