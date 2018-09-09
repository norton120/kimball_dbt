{% macro screen_partial(screen_collection,target_audit_properties) %}
{#
---- INTENT: populates the screen declaration and union statements
---- Args:
----    - screen_collection (list) the list of screen objects 
----    - target_audit_properties (dict) the dict containing the audit attributes
---- RETURNS: string collection of CTEs for all screens, union statement to merge them
#}

    WITH
---- screen_declaration iterates through each screen element in the collection and creates CTEs for them.     
    {{screen_declaration(screen_collection, target_audit_properties)}},

---- screen_union then merges the CTEs and preps them to be sequenced and added to error_event_fact.    
    {{screen_union_statement(screen_collection, target_audit_properties)}}

---- add a new error_event_key from the sequence for all the screens with > 1 record 
    SELECT
        sequence.nextval AS error_event_key,
        -- for some reason snowflake needs this explicitly cast
        audit_key::NUMBER AS audit_key,
        screen_name,
        error_subject,
        record_identifier,
        error_event_action
    FROM
        screen_union,
        TABLE(getnextval({{this.database}}.{{this.schema}}.quality_error_event_fact_pk_seq)) sequence
    WHERE
        audit_key IS NOT NULL

{% endmacro %}
