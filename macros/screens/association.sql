{%- macro association(screen_args, kwargs) -%}
{#
---- INTENT: screens for a required or impossible value in one column based on the value of another
---- Pass the screen_args object with these params:
----    - column (string) the name of the column to test
----    - depending_column (string) the name of the column to compare based on the column value
----    - column_value (string, integer, float, date) the value for the test column
----    - depending_value (string, integer, float, date) the value for the depending column
----    - not_equal (boolean) if True, depending column value is impossible, otherwise required 
----    - column_data_type (string) the type to cast the column value
----    - depending_data_type (string) the type to cast the depending value as
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
    {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column}}_association_{{screen_args.column_value}} AS (
        SELECT
            {{universal_audit_property_set('association_' + screen_args.column_value,screen_args,kwargs)}}

        AND
           ( {{screen_args.column}}::{{screen_args.column_data_type}} = 
        
        {% if screen_args.column_data_type | upper in ('STRING', 'TEXT', 'VARCHAR', 'DATE') %}
            '{{screen_args.column_value}}'
        {% else %}
            {{screen_args.column_value}}
        {% endif %}

        AND
            {{screen_args.depending_column}}::{{screen_args.depending_data_type}} 
            {{'=' if screen_args.not_equal else '<>'}}
         
        {% if screen_args.depending_data_type | upper in ('STRING', 'TEXT', 'VARCHAR', 'DATE') %}
            '{{screen_args.depending_value}}'
        {% else %}
            {{screen_args.depending_value}}
        {% endif %}
        ) 
    )
{%- endmacro -%}

