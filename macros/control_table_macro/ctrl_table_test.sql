{% macro ctrl_log_model1(control_table = 'HOSPITAL_DATA_MODEL.CONTROL_TABLE.CTRL_TAB_TEST') %}

    {% set run_started_at = context.get('run_started_at') %}
    {% set run_ended_at = modules.datetime.datetime.utcnow() %}
    {% set run_id = invocation_id %}
    {% set model_name = model.name %}
    {% set status = 'success' if execute else 'skipped' %}
    {% set run_duration = (run_ended_at.timestamp() - run_started_at.timestamp()) | round(2) %}

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


{% macro ctrl_log_results1(results, control_table='HOSPITAL_DATA_MODEL.CONTROL_TABLE.CTRL_TAB_TEST') %}

    {% set run_started_at = context.get('run_started_at') %}
    {% set run_ended_at = modules.datetime.datetime.utcnow() %}
    {% set run_id = invocation_id %}

    {% for result in results %}
        {% set model_name = result.node.name %}
        {% set status = result.status %}
        {% set run_duration = result.execution_time | default(0) %}

        {% do run_query(
            "insert into " ~ control_table ~ " (
                model_name, status, run_started_at, run_ended_at, run_duration, run_id
            ) values (
                '" ~ model_name ~ "',
                '" ~ status ~ "',
                '" ~ run_started_at ~ "',
                '" ~ run_ended_at ~ "',
                " ~ run_duration ~ ",
                '" ~ run_id ~ "'
            );"
        ) %}
    {% endfor %}

{% endmacro %}


{% macro get_last_run_time(model_name, control_table='HOSPITAL_DATA_MODEL.CONTROL_TABLE.CTRL_TAB_TEST') %}
    {% set query %}
        SELECT MAX(created_at) as last_run
        FROM {{ control_table }}
        WHERE model_name = '{{ model_name }}'
          AND status = 'success'
    {% endset %}

    {% set results = run_query(query) %}
    {% if results and results.columns[0].values() %}
        {% set result = results.columns[0].values()[0] %}
        {% if result is not none %}
            {% do return(result) %}
        {% else %}
            {% do return('2000-01-01 00:00:00') %}
        {% endif %}
    {% else %}
        {% do return('2000-01-01 00:00:00') %}
    {% endif %}
{% endmacro %}

