{%- macro date_range_within_history(screen_args, kwargs) -%}
{#
---- INTENT: screens for records that exist after this moment in time (future records) or before RevZilla.
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

    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_DATE_RANGE_WITHIN_HISTORY AS (
        SELECT
            {{universal_audit_property_set(screen_args.type,screen_args,kwargs)}}

        AND
            (
                    (
                            date_part('year', {{screen_args.column}}) >= 2007
                        AND
                            date_part('month', {{screen_args.column}}) >= 11
                        AND
                            {{screen_args.column}} < current_timestamp
                    )
                OR
                    {{screen_args.column}} IS NULL
            )
    )
{%- endmacro -%}
