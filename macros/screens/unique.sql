{%- macro unique_screen(screen_args, kwargs) -%}
{#
---- INTENT: screens for records where values are not unique 
---- Pass the screen_args object with these params:
----    - column (string) the name of the column to test
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
