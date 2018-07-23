---------- UNIQUE SCREEN
---- Verifies that each row in a column is unique in source data

{%- macro unique_screen(screen_args, kwargs) -%}
    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_UNIQUE AS (
        SELECT
            {{universal_audit_property_set(screen_args,kwargs)}}

        AND
            COUNT({{screen_args.column}}) - COUNT(DISTINCT({{screen_args.column}})) > 0
    )
{%- endmacro -%}
