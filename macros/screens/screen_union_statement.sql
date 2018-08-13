{% macro screen_union_statement(screen_args,kwargs) %}
{#
---- INTENT: creates the union statement for a given entity screen
---- Pass the screen_args object with these params:
----    - column (string) the name of the column to test
----    - type (string) the type of screen used
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
    {% for screen in screen_args %}
        SELECT
            audit_key,
            error_event_at,
            screen_name,
            error_subject,
            record_identifier,
            error_event_action
        FROM    
        
        -- association screens will have a screen_name value that includes the source column test value
        {% if screen.type == 'association' %}
            {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen.column}}_association_{{screen.column_value}}

        -- custom screens will have a screen_name value that overrides the normal type
        {% elif screen.type in ('custom','custom_aggregate') %}
            {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen.column}}_{{screen.screen_name}}

        -- for all other screens, the screen type (null, unique etc) is used to identify the cte
        {% else %} 
            {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen.column}}_{{screen.type}}        
        {% endif %}
        {{ 'UNION' if not loop.last}}
    {% endfor %}

{% endmacro %}
