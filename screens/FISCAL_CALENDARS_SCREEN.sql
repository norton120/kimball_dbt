{#---------- FISCAL_CALENDARS_SCREEN SCREEN
----
---- Screens are source-data-quality tests that we use to investigate and record data quality.     
---- You pass screens to the screen_collection list (below) for them to be run and error events collected. 

---- target_audit_properties contains meta about the current audit. it also accepts an exception_action key with
---- one of 4 values:
---- - Ignore : pass the record without action, but record the error
---- - Flag : pass the record but flag it as a quality issue 
---- - Reject : discard the record, record the error
---- - Halt : stop ETL process and sound alarm
---- default is Flag. 

---------- STATEMENTS [leave this section alone!]
---- Statements populate the python context with information about the subject audit.
#}
    {%- call statement('target_audit', fetch_result=True) -%}
        SELECT
            audit_key,
            cdc_target,
            lowest_cdc,
            highest_cdc,
            data_type
        FROM
            {{this.database}}.{{this.schema}}.audit
        LEFT JOIN
            "RAW".information_schema.columns
        ON
            table_schema = 'ERP'
        AND
            column_name = cdc_target
        AND
            table_name = entity_key
        WHERE
            database_key = 'RAW'
        AND
            schema_key = 'ERP'
        AND
            entity_key = 'FISCAL_CALENDARS'
        AND
            audit_status = 'In Process'
        ORDER BY audit_key DESC 
        LIMIT 1

    {%- endcall -%}

{% set audit_response_data_object = load_result('target_audit')['data']%}
{#---------- END STATMENTS #}

{# ---- if there is no new data, skip the entire screen model #}
{% if audit_response_data_object | length > 0 %}

    {%- set audit_response = audit_response_data_object[0] -%}
{# -- update the record identifier to match the table primary key #}

        {%- set target_audit_properties = {
                                'database' : 'RAW', 
                                'schema' : 'ERP',
                                'entity' : 'FISCAL_CALENDARS', 
                                'audit_key' :  audit_response[0],
                                'cdc_target' : audit_response[1],
                                'lowest_cdc' : audit_response[2],
                                'highest_cdc' : audit_response[3],
                                'cdc_data_type' : audit_response[4], 
                                'record_identifier' : 'id' } -%}
                        
{#
---------- SCREEN VARIABLES
#}

-------- DAY_OF_WEEK
---- not null
{% set day_of_week_not_null = {'column' : 'day_of_week', 'type' : 'not_null'} %}
---- values Monday - Sunday
{% set day_of_week_valid_days = {'column' : 'day_of_week', 'type' : 'valid_values', 'value_type' : 'TEXT', 'valid_values' : ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'], 'allow_null' : False} %}
 
-------- HOLIDAY
----
-------- ISO_MONTH
----
-------- WEEK_DAY_NAME
----
-------- FISCAL_PERIOD
----
-------- FISCAL_QTR
----
-------- ID
----
-------- _METADATA_FILE_MODIFIED_TS
----
-------- _METADATA_LINE_NUMBER
----
-------- HOLIDAY
----
-------- ISO_QTR
----
-------- WEEK_TYPE
----
-------- HOL_IND
----
-------- WEEK_DAY_NUMBER
----
-------- CALENDAR_YEAR
----
-------- FISCAL_YEAR
----
-------- ISO_YEAR
----
-------- IS_HOLIDAY
----
-------- _METADATA_TOTAL_ROWS
----
-------- ISO_QUARTER
----
-------- FISCAL_WEEK
----
-------- ISO_WEEK
----
-------- ISO_YEAR
----
-------- IS_FIRST_DAY_FISCAL_PERIOD
----
-------- XMIN__TEXT__BIGINT
----
-------- CALENDAR_MONTH
----
-------- _METADATA_FILE_NAME
----
-------- FISCAL_YEAR
----
-------- CALENDAR_QUARTER
----
-------- FISCAL_WEEK
----
-------- ISO_WEEK
----
-------- FISCAL_PERIOD
----
-------- _METADATA__TIMESTAMP
----
-------- WEEK_DAY_NUM
----
-------- _METADATA__UUID
----
-------- DATE
----
-------- ISO_MONTH
----
-------- _METADATA_CONSOLIDATION
----
-------- FISCAL_QUARTER
----
-------- _METADATA__TIMESTAMP
----
-------- _METADATA__UUID
----
-------- DATE



{#
---------- COLLECT VARIABLES
---- add each screen variable above to the collection
#}
    {% set screen_collection =  [
                                    day_of_week_not_null,
                                    day_of_week_valid_days

                                ]%}

{# ---------- RUN SCREENS [leave this section alone!] #}
WITH
        {{screen_declaration(screen_collection, target_audit_properties)}}


{# ---------- UNION [leave this section alone!] #}

    SELECT
        *
    FROM
        (
            {{screen_union_statement(screen_collection, target_audit_properties)}}

        )


{% else %}

{# ---- when no new data is present, return an empty table #}
    SELECT
        *
    FROM 
        {{this.database}}.{{this.schema}}.error_event_fact
    WHERE 1=0
{% endif %} 

{# ---------- CONFIGURATION [leave this section alone!] #}
{{config({

    "materialized":"ephemeral",
    "sql_where":"TRUE",
    "schema":"QUALITY"

})}}
    
