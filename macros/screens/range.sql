{%- macro range(screen_args, kwargs) -%}

{#
---- INTENT: screens for values within a given range. Ignores NULL values.
---- Pass the screen_args object with these params:
----    - column (string) the name of the column to test
----    - cast_as (string) data type to cast the column as
----    - range_start (number, date) lowest value to allow
----    - range_end (number, date) highest value to allow
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
---- RETURNS: string CTE of failing condition rows
#}
    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_RANGE AS (
        SELECT
            {{universal_audit_property_set('range',screen_args,kwargs)}}

        AND
            {{screen_args.column}} NOT BETWEEN
            {% if screen_args.cast_as.upper() in ('DATE','TIMESTAMP_LTZ','TIMESTAMP_NTZ') %}
                '{{screen_args.range_start}}' AND '{{screen_args.range_end}}'
            {% else %}
                {{screen_args.range_start}} AND {{screen_args.range_end}}
            {% endif %}
        AND
            {{screen_args.column}} IS NOT NULL
    )
{%- endmacro -%}
