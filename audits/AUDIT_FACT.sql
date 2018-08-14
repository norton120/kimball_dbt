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
        NULL::NUMBER AS audit_key,
        NULL::NUMBER AS gross_record_count,
        NULL::NUMBER AS validated_record_count,
        NULL::TIMESTAMP_LTZ AS audit_completed_at,
        NULL::NUMBER AS audit_date_key
    WHERE
        audit_key IS NOT NULL
{% endif %}




{#---------- DEPENDENCY HACK #}
---- {{ref('ERROR_EVENT_FACT')}}
{#---------- CONFIGURATIONT #}

{{config({
    "materialized":"incremental",
    "sql_where":"TRUE",
    "schema":"QUALITY",
    "post-hook": [
        "UPDATE {{this.database}}.{{this.schema}}.audit SET audit_status = 'Completed' WHERE audit_key IN
            (SELECT
                audit_key
            FROM
                {{this.database}}.{{this.schema}}.audit_fact)",
            

            "{{comment({'grain' : 'One row per completed audit.','definition' : 'KPIs for each audit run.'})}}",

            "{{comment({'column' : 'AUDIT_DATE_KEY','definition' : 'The FK to the date when audit was run.', 'additive' : false})}}",
            "{{add_constraints(['Fkey', 'Null'], this.schema, 'AUDIT_FACT', 'AUDIT_DATE_KEY', 'DATE', 'DATE_KEY', 'incremental')}}",

            "{{comment({'column' : 'AUDIT_KEY','definition' : 'The FK to the audit dim.', 'additive' : false})}}",
            "{{add_constraints(['Fkey', 'Null'], this.schema, 'AUDIT_FACT', 'AUDIT_KEY', 'AUDIT', 'AUDIT_KEY', 'incremental')}}",

            "{{comment({'column' : 'AUDIT_COMPLETED_AT','definition' : 'The timestamp completion of the audit.', 'additive' :false})}}",
            "{{add_constraints(['Null'], this.schema, 'AUDIT_FACT', 'AUDIT_COMPLETED_AT', None, None, 'incremental')}}",

            "{{comment({'column' : 'VALIDATED_RECORD_COUNT','definition' : 'The number of records in this audit that passed without error.', 'additive' :true})}}",

            "{{comment({'column' : 'GROSS_RECORD_COUNT','definition' : 'The number of total records audited.', 'additive' :true})}}"
 
    ]

})}}
