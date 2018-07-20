---------- ERROR EVENT
---- AS screens are applied during audits they can generate errors.
---- These errors are collected and added to the error event table here.
---- For each screen model, add as a ref and then union the results.

---------- FORMATTING
---- To help keep this from becoming a mess, follow these rules: 
---- * 3 newlines between CTEs
---- * Keep refs in the union statment in alphabetical order. Yes it makes git diffs harder to read.
WITH
unioned_error_events AS (
    SELECT
        audit_key,
        screen_name,
        error_subject,
        record_identifier,
        error_event_action 
    FROM
        {{ref('ORDERS_SCREEN')}}   
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
