---------- BLACKLIST SCREEN
---- Verifies that blacklisted values are not included

{%- macro blacklist(screen_args, kwargs) -%}
---- Pass the screen_args object with these params:
---- screen_args:
----    - blacklisted_values (list) the values to deny
----    - value_type (string) the datatype for the list of values

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
    )
{%- endmacro -%}
