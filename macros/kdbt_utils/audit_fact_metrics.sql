{%- macro audit_fact_metrics(audits) -%}
{#
---- INTENT: create the aggregated metrics that become an entry in the audit_fact table
---- ARGS:
----    - audits (list) a list of touples representing an audit.
----        signature for each touple is (audit_key, database, schema, entity, cdc_column, min_cdc, max_cdc, cdc_data_type)
----            - audit_key (integer) the audit identifier
----            - database (string) the subject database
----            - schema (string) the subject schema
----            - entity (string) the name of the subject table or view
----            - cdc_target (string) the name of the cdc column
----            - min_cdc (string) the stringified value of the lowest cdc column   
----            - max_cdc (string) the stringified value of the highest cdc column
----            - cdc_data_type (string) the data type to cast the min and max values 
#}
    WITH  
    {% for audit_row in audits %}
        audit_{{audit_row[0]}}_metrics AS (
            SELECT
                {{audit_row[0]}} AS audit_key,
                (SELECT 
                    COUNT(*) 
                FROM 
                    {{audit_row[1]}}.{{audit_row[2]}}.{{audit_row[3]}}
                WHERE
                    {{audit_row[4]}}::{{audit_row[7]}}
                BETWEEN
                
                {% if audit_row[7] == 'TEXT','TIMESTAMP_NTZ' %}
                    '{{audit_row[5]}}' AND '{{audit_row[6]}}'
                {% else %}
                    {{audit_row[5]}} AND {{audit_row[6]}}
                {% endif %}
                ) AS gross_record_count,

                (SELECT
                    COUNT(*)
                FROM
                    {{this.database}}.{{this.schema}}.ERROR_EVENT_FACT
                WHERE 
                    audit_key = {{audit_row[0]}}) AS error_event_count
        )
        {{ ',' if not loop.last }}  
    {% endfor %}
    
    SELECT
        audit_key,
        gross_record_count,
        error_event_count
    FROM
        (
        {% for audit_union in audits %}
            SELECT
                audit_key,
                gross_record_count,
                error_event_count
            FROM
               audit_{{audit_union[0]}}_metrics
            
            {{ 'UNION' if not loop.last }}  
        {% endfor %}
        )
{%- endmacro -%}
