{% macro ctrl_log_model(control_table = 'HOSPITAL_DATA_MODEL.CONTROL_TABLE.CTRL_TAB' )%}
 
    {% set run_started_at = context.get('run_started_at') %}
    {% set run_ended_at = modules.datetime.datetime.utcnow() %}
    {% set run_id = invocation_id %}
    {% set model_name = model.name %}
    {% set status = 'success' if execute else 'skipped' %}
    {% set run_duration = (
        run_ended_at.timestamp() - run_started_at.timestamp()
    ) | round(2) %}
 
    insert into {{ control_table }} (
        model_name,
        status,
        run_started_at,
        run_ended_at,
        run_duration,
        run_id
    )
    values (
        '{{ model_name }}',
        '{{ status }}',
        '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}',
        '{{ run_ended_at.strftime("%Y-%m-%d %H:%M:%S") }}',
        {{ run_duration }},
        '{{ run_id }}'
    );
 
{% endmacro %}




{% macro ctrl_log_results(results, control_table='HOSPITAL_DATA_MODEL.CONTROL_TABLE.CTRL_TAB') %}
 
    {% set run_started_at = context.get('run_started_at') %}
    {% set run_ended_at = modules.datetime.datetime.utcnow() %}
    {% set run_id = invocation_id %}
 
    {% for result in results %}
        {% set model_name = result.node.name %}
        {% set status = result.status %}
        {% set run_duration = result.execution_time | default(0) %}
 
        insert into {{ control_table }} (
            model_name,
            status,
            run_started_at,
            run_ended_at,
            run_duration,
            run_id
        )
        values (
            '{{ model_name }}',
            '{{ status }}',
            '{{ run_started_at }}',
            '{{ run_ended_at }}',
            {{ run_duration }},
            '{{ run_id }}'
        );
    {% endfor %}
 
{% endmacro %}
 




