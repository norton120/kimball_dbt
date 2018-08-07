{%- macro valid_name(screen_args, kwargs) -%}
{#
---- INTENT: screens for values with an invalid English naming convention
----    note: We are using English base conventions to simplify support at the BI level. 
----    Non-UTF characters are perfectly acceptable but will be flagged by this macro.
----
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
    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_VALID_NAME AS (
        SELECT
            {{universal_audit_property_set('valid_name',screen_args,kwargs)}}

        AND
            {{screen_args.column}} IS NULL
        AND
            {{screen_args.column}} NOT REGEXP '[\s a-z A-Z \'\' \-]*'
    )
{%- endmacro -%}
