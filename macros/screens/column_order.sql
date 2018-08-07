{%- macro column_order(screen_args, kwargs) -%}
{#
---- INTENT: screens for records where columns are not in proper value order with each other
---- Pass the screen_args object with these params:
----    - column (string) the name of the column to test
----    - greater_column (number) the column with the expected greater value. 
----    - lesser_column (string) the column with the expected lower value. 
----    - data_type (string) the data type to cast both columns.
----    - equal (boolean) do equal values pass or fail. Default fail. 
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


    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_COLUMN_ORDER AS (
        SELECT
            {{universal_audit_property_set('column_order',screen_args,kwargs)}}
        
        AND
            {{screen_args.greater_column}}::{{screen_args.data_type}} 
                {%- if screen_args.equal -%}
                    <
                {%- else -%}
                    <= 
                {%- endif -%}
            {{screen_args.lesser_column}}::{{screen_args.data_type}}
        AND 
            {{screen_args.greater_column}} IS NOT NULL
        AND
            {{screen_args.lesser_column}} IS NOT NULL
    )
{%- endmacro -%}
