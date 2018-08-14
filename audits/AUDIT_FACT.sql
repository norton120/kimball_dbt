---------- AUDIT FACT
---- After an audit run is completed, we compile the metrics and KPIs for the run
---- here in AUDIT FACT. We also use the post-hook to set the run status to completed.

---------- FORMATTING
---- To help keep this from becoming a mess, follow these rules:
---- * 3 newlines between CTEs


---------- INITIAL STATEMENT
{% call statement('audit_fact', fetch_result=True) %}
---- INTENT: captures all the details of open audits and populates the jinja context.
---- ARGS: none
---- RETURNS: dict of lists, each list containing the comma-deliniated values for each row

    SELECT
        audit_key,
        database_key,
        schema_key,
        entity_key,
        cdc_target,
        lowest_cdc,
        highest_cdc,
        data_type

    FROM
        {{this.database}}.{{this.schema}}.audit
    LEFT JOIN
        raw.information_schema.columns
    ON
        table_schema = schema_key
    AND
        column_name = cdc_target
    AND
        table_name = entity_key
    WHERE
        audit_status = 'In Process'
{% endcall %}


---- load the values from the audit_fact call into a local context for use
{%- set audit_fact_response = load_result('audit_fact')['data'] -%}


---- if no rows are returned, increment with an empty table
{% if audit_fact_response | length > 0 %}
---- get the total record count within the audit context
    WITH
    all_records_in_audit_context AS (
        {{audit_fact_metrics(audit_fact_response)}}
    )

    SELECT
        audit_key,
        gross_record_count,
        gross_record_count - error_event_count AS validated_record_count,
        CURRENT_TIMESTAMP() AS audit_completed_at,
        {{date_key('CURRENT_DATE()')}} AS audit_date_key
    FROM
        all_records_in_audit_context
{% else %}
    SELECT
        NULL AS audit_key,
        NULL AS gross_record_count,
        NULL AS validated_record_count,
        NULL AS audit_completed_at,
        NULL AS audit_date_key
    WHERE
        audit_key IS NOT NULL
{% endif %}



<<<<<<< HEAD
{#---------- DEPENDENCY HACK weirdly if you move this it will bork #}
---- {{ref('ERROR_EVENT_FACT')}}
=======
{#
---------- DEPENDENCY HACK
---- {{ref('ERROR_EVENT_FACT')}}
#}


>>>>>>> 0ef8c4039e0701f5775aa48fb56b5d826fe3cfa1

{{config({
    "materialized":"incremental",
    "sql_where":"TRUE",
    "schema":"QUALITY",
    "post-hook": [
        "UPDATE {{this.database}}.{{this.schema}}.audit SET audit_status = 'Completed' WHERE audit_key IN
            (SELECT
                audit_key
            FROM
                {{this.database}}.{{this.schema}}.audit_fact)"
    ]

})}}
<<<<<<< HEAD



=======
>>>>>>> 0ef8c4039e0701f5775aa48fb56b5d826fe3cfa1
