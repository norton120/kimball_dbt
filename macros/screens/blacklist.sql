{%- macro blacklist(screen_args, kwargs) -%}
{#
---- INTENT: screens for values in a list of known bad values
----    - does not screen for null values
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
---- RETURNS: string CTE of failing condition rows
#}
    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_blacklist AS (
        SELECT
            {{universal_audit_property_set('blacklist',screen_args,kwargs)}}

        AND
            {{screen_args.column}} IN (
            {%- for val in screen_args.blacklist_values -%}
                {%- if screen_args.value_type.upper() in ('VARCHAR','STRING','TEXT','TIMESTAMP_LTZ','TIMESTAMP_NTZ') -%}
                    '{{val}}'
                {%- else -%}
                    {{val}}
                {%- endif -%}
                {{ ',' if not loop.last}}
            {%- endfor -%}
            )

        AND
            {{screen_args.column}} IS NOT NULL
    )
{%- endmacro -%}
