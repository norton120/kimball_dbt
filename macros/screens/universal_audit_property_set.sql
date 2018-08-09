{% macro universal_audit_property_set(screen_type,screen_args,kwargs) %}
{#
---- INTENT: Nearly all screens need the same property set as a base.
----    This generates the property set and leaves the end of the CTE open (no parenthesis)
----    So it can be extended using AND values for the WHERE clause.
----
---- screen_type (string) the name of the applied screen type
----
---- Pass the screen_args object with these params:
----    - column (string) the name of the column to test
----    - blacklisted_values (list) the values to deny
----    - value_type (string) the datatype for the list of values
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
---- RETURNS: string of boilerplate columns for screens
#}

        {{kwargs.audit_key}} AS audit_key,
            CURRENT_TIMESTAMP() AS error_event_at,
            '{{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column | upper}}_{{screen_type | upper}}' AS screen_name,
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
{% endmacro %}
