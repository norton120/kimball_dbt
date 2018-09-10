---------- ERROR EVENT
---- creates the schema for the ERROR_EVENT_FACT table.

    SELECT
        NULL::NUMBER AS error_event_key,
        NULL::NUMBER AS audit_key,
        NULL::VARCHAR AS screen_name,
        NULL::VARCHAR AS error_subject,
        NULL::VARCHAR AS record_identifier,
        NULL::VARCHAR AS error_event_action
    WHERE
        audit_key IS NOT NULL

{#---- DEPENDENCY HACK  #}
---- {{ref('AUDIT')}}

{#---------- CONFIGURATION #}

    {{config({
        "materialized":"incremental",
        "sql_where":"TRUE",
        "schema":"QUALITY",
        "post-hook":[
            "{{comment({'column':'error_event_key','definition':'PK unique to every error event.', 'additive' : false})}}",
            "{{add_constraints(['Null','Pkey'],this.schema, 'ERROR_EVENT_FACT', 'ERROR_EVENT_KEY', None, None, 'incremental')}}",

            "{{comment({'column':'audit_key','definition':'FK to the audit that generated the error event.', 'additive' : false})}}",
            "{{add_constraints(['Fkey','Null'],this.schema, 'ERROR_EVENT_FACT', 'AUDIT_KEY', 'AUDIT', 'AUDIT_KEY', 'incremental')}}",

            "{{comment({'column':'screen_name','definition':'Name of the screen that matches the CTE name in the screen model.', 'additive' : false})}}",
            "{{add_constraints(['Null'],this.schema,'ERROR_EVENT_FACT', 'SCREEN_NAME', None, None, 'incremental')}}",

            "{{comment({'column':'error_subject','definition':'The object attribute or entity that failed the given screen. Can be a column, table, or combination.', 'additive' : false})}}",
            "{{add_constraints(['Null'], this.schema, 'ERROR_EVENT_FACT', 'ERROR_SUBJECT', None, None, 'incremental')}}",

            "{{comment({'column':'record_identifier','definition':'The PK of the record that failed the screen. For entities Not Applicable.', 'additive' : false})}}",
            "{{add_constraints(['Null'], this.schema, 'ERROR_EVENT_FACT', 'RECORD_IDENTIFIER', None, None, 'incremental')}}",
            "{{comment({'column':'error_event_action','definition':'The action taken in response to the instance failing the screen.', 'additive' : false})}}",
            "{{add_constraints(['Null'], this.schema, 'ERROR_EVENT_FACT', 'ERROR_EVENT_ACTION', None, None, 'incremental')}}",

            "{{comment({'definition':'Every time an instance or entity fails a screen, an error event is created.',
                        'grain':'Every time an instance or entity fails a screen, an error event is created.'})}}"












   ]})}}

