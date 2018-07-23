---------- AUDIT FACT
---- After an audit run is completed, we compile the metrics and KPIs for the run
---- here in AUDIT FACT. We also use the post-hook to set the run status to completed. 

---------- FORMATTING
---- To help keep this from becoming a mess, follow these rules: 
---- * 3 newlines between CTEs

{% call statement('audit_fact', fetch_result=True) %}
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
        {{this.database}}.quality.audit 
    LEFT JOIN
-- TODO: this breaks if we ever expand the data lake to more than the raw db
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

{%- set audit_fact_response = load_result('audit_fact')['data'] -%}

---- get the total record count within the audit context
WITH
all_records_in_audit_context AS (
    {{audit_fact_metrics(audit_fact_response)}}
)

SELECT
    audit_key,
    gross_record_count,
    gross_record_count - error_event_count AS validated_record_count
FROM
    all_records_in_audit_context

{{config({
    "materialized":"incremental",
    "sql_where":"TRUE",
    "schema":"QUALITY",
    "post-hook": [
        "UPDATE {{this.database}}.quality.audit SET audit_status = 'Completed' WHERE audit_key IN 
            (SELECT
                audit_key
            FROM
                {{this.database}}.quality.audit_fact)"
    ]

})}}


---------- DEPENDENCY HACK
---- {{ref('ERROR_EVENT_FACT')}}
 
        
