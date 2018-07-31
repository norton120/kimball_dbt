---------- values_at_least SCREEN
---- Verifies that each row in a column is greater than or equal a provided value, if not null.

---------- Signature
----
----
----


{%- macro null_screen(screen_args, kwargs) -%}
    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_VALUES_AT_LEAST_{{kwargs.provided_value}} AS (
        SELECT
            {{universal_audit_property_set('VALUES_AT_LEAST_{{kwargs.provided_value}}',screen_args,kwargs)}}

        AND
            (
                    {{screen_args.column}} >= {{kwargs.provided_value}}
                OR
                    {{screen_args.column}} IS NULL
            )
    )
{%- endmacro -%}
