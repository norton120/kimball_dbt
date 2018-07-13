---------- NOT NULL SCREEN
---- Verifies that no null values are present in source data

{%- macro null_screen() -%}
    {{target_path.database}}_{{target_path.schema}}_{{target_path.entity}}_{{kwargs.column}}_NOT_NULL AS (
        SELECT
            CURRENT_TIMESTAMP() AS error_event_at,
            {{audit_key}} AS audit_key,
            '{{target_path.database}}_{{target_path.schema}}_{{target_path.entity}}_{{kwargs.column}}_NOT_NULL' AS screen_name,
            '{{kwargs.column}}' AS error_subject,
            {{kwargs.primary_key}} AS record_identifier,
            
        {% if kwargs.exception_action %}                 
            '{{kwargs.exception_action' AS error_event_action
        {% else %}
            'Flag' AS error_event_action
        FROM
            {{target_path.database}}.{{target_path.schema}}.{{target_path.entity}}               
        WHERE
            {{cdc_target}} BETWEEN {{lowest_cdc}} AND {{highest_cdc}}
    )  
