{% macro screen_union_statement(screen_args,kwargs) %}
---- INTENT: creates the union statement for a given entity screen
----    ARGS:
----        - screen_args(list) a list of screen objects to build into cte names
----        - kwargs(object) the target audit object
---- RETURNS: string the union statment

    {% for screen in screen_args %}
        SELECT
            audit_key,
            error_event_at,
            screen_name,
            error_subject,
            record_identifier,
            error_event_action
        FROM    
        {{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen.column}}_{{screen.type}}        

        {{ 'UNION' if not loop.last}}
    {% endfor %}

{% endmacro %}
