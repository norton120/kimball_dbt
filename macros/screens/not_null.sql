---------- NOT NULL SCREEN
---- Verifies that no null values are present in source data

{%- macro null_screen(screen_args, kwargs) -%}
    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_NOT_NULL AS (
        SELECT
            {{universal_audit_property_set('null',screen_args,kwargs)}}

        AND
            {{screen_args.column}} IS NULL
    )
{%- endmacro -%}
