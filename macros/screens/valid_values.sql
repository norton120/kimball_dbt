{%- macro valid_values(screen_args, kwargs) -%}

{#
---- INTENT: screens for values within a whitelist
----    - does not screen for null values
---- Pass the screen_args object with these params:
----    - column (string) the name of the column to test
----    - valid_values (list) the values to allow
----    - value_type (string) the datatype for the list of values
----    - allow_null (boolean) adds NULL to the list of allowed values
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
    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_VALID_VALUES AS (
        SELECT
            {{universal_audit_property_set('valid_values',screen_args,kwargs)}}

        AND
            {{screen_args.column}} NOT IN (
            {%- for val in screen_args.valid_values -%}
                {%- if screen_args.value_type.upper() in ('STRING','TEXT','TIMESTAMP_LTZ','TIMESTAMP_NTZ') -%}
                    '{{val}}'
                {%- else -%}
                    {{val}}
                {%- endif -%}
                {{ ',' if not loop.last}}
            {%- endfor -%}
            {{ ', NULL' if screen_args.allow_null else ''}}
            )

        AND
            {{screen_args.column}} IS NOT NULL
    )
{%- endmacro -%}
