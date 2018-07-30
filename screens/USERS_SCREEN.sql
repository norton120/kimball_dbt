---------- USERS_SCREEN SCREEN
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
            entity_key = 'DW_USERS_VIEW'
        AND
            audit_status = 'In Process'
        ORDER BY audit_key DESC 
        LIMIT 1

    {%- endcall -%}
{% set audit_response_data_object = load_result('target_audit')['data']%}
---------- END STATMENTS

---- if there is no new data, skip the entire screen model
{% if audit_response_data_object | length > 0 %}

    {%- set audit_response = audit_response_data_object[0] -%}
-- update the record identifier to match the table primary key

        {%- set target_audit_properties = {
                                'database' : 'RAW', 
                                'schema' : 'ERP',
                                'entity' : 'DW_USERS_VIEW', 
                                'audit_key' :  audit_response[0],
                                'cdc_target' : audit_response[1],
                                'lowest_cdc' : audit_response[2],
                                'highest_cdc' : audit_response[3],
                                'cdc_data_type' : audit_response[4], 
                                'record_identifier' : 'id' } -%}
                        

---------- SCREENS

    {% set id_not_null = {'column':'ID','type':'not_null'} %} 
    {% set last_name_unique = {'column':'LAST_NAME','type':'unique'} %}
    {% set screen_collection =  [
                                    id_not_null,
                                    last_name_unique
                                ]%}

    WITH

        {{screen_declaration(screen_collection, target_audit_properties)}}


---------- UNION
    SELECT
        *
    FROM
        (
            {{screen_union_statement(screen_collection, target_audit_properties)}}

        )

{% else %}

---- when no new data is present, return an empty table
    SELECT
        *
    FROM 
        {{this.database}}.{{this.schema}}.error_event_fact
    WHERE 1=0
{% endif %} 



---------- MODEL CONFIGURATION
{{config({

    "materialized":"ephemeral",
    "sql_where":"TRUE",
    "schema":"QUALITY"

})}}
    
