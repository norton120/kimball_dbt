---------- VALID VALUES SCREEN
---- Verifies that only whitelisted values are included

{%- macro valid_values(screen_args, kwargs) -%}
---- Pass the screen_args object with these params:
---- screen_args:
----    - valid_values (list) the values to allow
----    - value_type (string) the datatype for the list of values
----    - allow_null (boolean) adds NULL to the list of allowed values

    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_VALID_VALUES AS (
        SELECT
            {{universal_audit_property_set('valid_values',screen_args,kwargs)}}

        AND
            {{screen_args.column}} IN (
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
    )
{%- endmacro -%}
