---------- values_at_least SCREEN
---- Verifies that each row in a column is greater than or equal a provided value, if not null.

{%- macro values_at_least(screen_args, kwargs) -%}
---- Pass the screen_args object with these params:
---- screen_args:
----    - column is the field to screen on
----    - provided_value (numeric, integer, etc.) is the minimum value allowed

    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_VALUES_AT_LEAST AS (
        SELECT
            {{universal_audit_property_set('values_at_least',screen_args,kwargs)}}

        AND
            (
                    {{screen_args.column}} >= {{screen_args.provided_value}}
                OR
                    {{screen_args.column}} IS NULL
            )
    )
{%- endmacro -%}
