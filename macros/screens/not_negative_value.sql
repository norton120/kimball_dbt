---------- not_negative_value SCREEN
---- Verifies that each row in a column does not contain a value less than 0 (zero).

---------- Signature
----
----
----


{%- macro null_screen(screen_args, kwargs) -%}
    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_NOT_NEGATIVE_VALUE AS (
        SELECT
            {{universal_audit_property_set('not_negative_value',screen_args,kwargs)}}

        AND
            (
                    {{screen_args.column}} >= 0
                OR
                    {{screen_args.column}} IS NULL
            )
    )
{%- endmacro -%}
