---------- STATIC VALUE AFTER SCREEN
---- After a given record date, all records should have a set value for 
---- the given column.

{%- macro static_value_after(screen_args, kwargs) -%}
---- Pass the screen_args object with these params:
----    - column (string) the column tested for the static value
----    - date_column (string) the column for the date predicate
----    - before (date) the date to restrict the date_column by
----    - column_value (string) the value to expect for the column givent the predicate. Default NULL
---     - column_data_type (string) the type to cast the column to 
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
    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_STATIC_VALUE_AFTER AS (
        SELECT
            {{universal_audit_property_set('static_value_after',screen_args,kwargs)}}

        AND
            {% if screen_args.column_value %}
                {% if screen_args.column_data_type in ('TEXT','STRING','VARCHAR','DATE') %}
                    {{screen_args.column}} <> '{{screen_args.column_value}}'::{{screen_args.column_data_type}} 
                {% else %}
                    {{screen_args.column}} <> {{screen_args.column_value}}::{{screen_args.column_data_type}}
                {% endif %}
            {% else %}
                {{screen_args.column}} IS NOT NULL
            {% endif %}
        AND
            {{screen_args.date_column}} > '{{screen_args.before}}'
    )
{%- endmacro -%}
