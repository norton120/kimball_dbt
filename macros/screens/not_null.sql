---------- NOT NULL SCREEN
---- Verifies that no null values are present in source data

{%- macro null_screen(screen_args, kwargs) -%}
    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_NOT_NULL AS (
        SELECT
        {{kwargs.audit_key}} AS audit_key,
            CURRENT_TIMESTAMP() AS error_event_at,
            '{{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column | upper}}_NOT_NULL' AS screen_name,
            '{{screen_args.column | upper}}' AS error_subject,
        
        {% if kwargs.audit_key %}
            {{kwargs.record_identifier}} AS record_identifier,
        {% else %}
            'Not Applicable' AS record_identifier,
        {% endif %}        
    
        {% if screen_args.exception_action %}                 
            '{{screen_args.exception_action}}' AS error_event_action
        {% else %}
            'Flag' AS error_event_action
        {% endif %}

        FROM
            {{kwargs.database}}.{{kwargs.schema}}.{{kwargs.entity}} 
        WHERE
            {{kwargs.cdc_target}}::{{kwargs.cdc_data_type}} 
        BETWEEN 
        {% if kwargs.cdc_data_type in ('TIMESTAMP_NTZ','TEXT') %}
            '{{kwargs.lowest_cdc}}' AND '{{kwargs.highest_cdc}}'
        {% else %}
            {{kwargs.lowest_cdc}} AND {{kwargs.highest_cdc}}
        {% endif %}
    )  
{%- endmacro -%}
