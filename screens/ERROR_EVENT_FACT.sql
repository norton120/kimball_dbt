---------- ERROR EVENT
---- AS screens are applied during audits they can generate errors.
---- These errors are collected and added to the error event table here.
---- to use, just add the prefix of the screen you want to the all_screens variable. 
---- for example, if you want to add the ORDERS_SCREEN.sql results, add 'ORDERS' to the all_screens variable.

---------- FORMATTING
---- keep the values in the all_screens list in alphabetical order so they are easy to search through.  

---- INCLUDE SCREENS BY ADDING THEM HERE:
{% set all_screens = [
                    'FISCAL_CALENDARS',
                    'PRODUCTS',
                    'USERS'
                    ] %}



---------- NOTHING TO CHANGE BELOW HERE
---- this is all framework from here down. 


WITH
unioned_error_events AS (
    SELECT 
        audit_key,
        screen_name,
        error_subject,
        record_identifier,
        error_event_action 
    FROM
        
{% for screen in all_screens %}
    (
    SELECT
        audit_key,
        screen_name,
        error_subject,
        record_identifier,
        error_event_action 
    FROM
        {{ref(screen|upper +'_SCREEN')}}   
    )
    {{ 'UNION' if not loop.last }}

{% endfor %}


)
---- create the final partial    
    SELECT
        sequence.nextval AS error_event_key,
        audit_key,
        screen_name,
        error_subject,
        record_identifier,
        error_event_action    
    FROM
        unioned_error_events,
        TABLE(getnextval(quality_error_event_fact_pk_seq)) sequence  
    WHERE
        audit_key IS NOT NULL



---------- CONFIGURATION
    {{config({
        "materialized":"incremental",
        "sql_where":"TRUE",
        "schema":"QUALITY",
        "post-hook":[
            "{{comment({'column':'error_event_key','definition':'PK unique to every error event.'})}}",
            "{{comment({'column':'audit_key','definition':'FK to the audit that generated the error event.'})}}",
            "{{comment({'column':'screen_name','definition':'Name of the screen that matches the CTE name in the screen model.'})}}",
            "{{comment({'column':'error_subject','definition':'The object attribute or entity that failed the given screen. Can be a column, table, or combination.'})}}",
            "{{comment({'column':'record_identifier','definition':'The PK of the record that failed the screen. For entities Not Applicable.'})}}",
            "{{comment({'column':'error_event_action','definition':'The action taken in response to the instance failing the screen.'})}}",

            "{{comment({'definition':'Every time an instance or entity fails a screen, an error event is created.', 
                        'grain':'Every time an instance or entity fails a screen, an error event is created.'})}}"    
    
   ]})}}

---------- DEPENDENCY HACK
---- {{ref('AUDIT')}}
