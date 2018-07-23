{%- macro audit_fact_metrics(audits) -%}
   
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
                    {{this.database}}.QUALITY.ERROR_EVENT_FACT
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
