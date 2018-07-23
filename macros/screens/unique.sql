---------- UNIQUE SCREEN
---- Verifies that each row in a column is unique in source data

{%- macro unique_screen(screen_args, kwargs) -%}
    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_UNIQUE AS (
        SELECT
            {{universal_audit_property_set('unique', screen_args,kwargs)}}

        AND
            id IN
                (
                    SELECT
                        id
                    FROM
                        {{kwargs.database}}.{{kwargs.schema}}.{{kwargs.entity}}
                    WHERE
                        {{screen_args.column}} IN
                            (
                                SELECT
                                    {{screen_args.column}}
                                FROM
                                    (
                                        SELECT
                                            {{screen_args.column}},
                                            (COUNT(*)) AS unique_test
                                        FROM
                                            {{kwargs.database}}.{{kwargs.schema}}.{{kwargs.entity}}
                                        GROUP BY
                                            {{screen_args.column}}
                                        HAVING
                                            unique_test > 1
                                    )
                            )
                )
    )
{%- endmacro -%}
