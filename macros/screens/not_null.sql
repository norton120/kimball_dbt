---------- NOT NULL SCREEN
---- Verifies that no null values are present in source data

{%- macro null_screen(target_path, kwargs) -%}
    {{target_path.database}}_{{target_path.schema}}_{{target_path.entity}}_{{kwargs.column}}_NOT_NULL AS (
        SELECT
            CURRENT_TIMESTAMP() AS error_event_at,
            target_audit.audit_key,
            '{{target_path.database}}_{{target_path.schema}}_{{target_path.entity}}_{{kwargs.column}}_NOT_NULL' AS screen_name,
            '{{kwargs.column}}' AS error_subject,
            {{kwargs.primary_key}} AS record_identifier,
            
        {% if kwargs.exception_action %}                 
            '{{kwargs.exception_action}}' AS error_event_action
        {% else %}
            'Flag' AS error_event_action
        {% endif %}
        FROM
            {{target_path.database}}.{{target_path.schema}}.{{target_path.entity}} 
        JOIN
            target_audit
        ON 1=1              
        WHERE

-- TODO: how do we dynamically set the value for cdc_target? Does this need a DBT statement function?

            {{cdc_target}} BETWEEN {{lowest_cdc}} AND {{highest_cdc}}
    )  
{%- endmacro -%}
