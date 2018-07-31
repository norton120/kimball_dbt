---------- CUSTOM SCREEN
---- This screen allows for complex business logic tests

{%- macro custom(screen_args, kwargs) -%}
---- Pass the screen_args object with these params:
---- screen_args:
----    - sql_where (string) the WHERE clause that defines a failing screen
----    - screen_name (string) the custom name for the screen
----    - column (string) the column tested. When more than one column is tested select one primary.
---- Pass the kwargs object with these params:
----    - database (string) the source database 
----    - schema (string) the source schema
----    - entity (string) the source table / view name
----    - audit_key (integer) the Fkey to the audit being performed
----    - cdc_target (string) the column used to indicate change in the entity
----    - lowest_cdc (string) the lowest cdc_target value in this audit
----    - highest_cdc (string) the highest cdc_target value in this audit
----    - cdc_data_type (string) the native data type of the cdc_column in the source entity
----    - record_identifier (string) the primary key for the source entity
    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_{{screen_args.screen_name}} AS (
        SELECT
            {{kwargs.audit_key}} AS audit_key,
            CURRENT_TIMESTAMP() AS error_event_at,
            '{{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column | upper}}_{{screen_args.screen_name | upper}}' AS screen_name,
            '{{screen_args.column | upper}}' AS error_subject,

        {% if kwargs.record_identifier %}
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

        AND
            {{screen_args.sql_where}} 
    )
{%- endmacro -%}
