---------- ORDERS_SCREEN SCREEN
----
---- Screens are source-data-quality tests that we use to investigate and record data quality.     
---- You pass screens to the screen_collection list (below) for them to be run and error events collected. 

---- target_audit_properties contains meta about the current audit. it also accepts an exception_action key with
---- one of 4 values:
--- - Ignore : pass the record without action, but record the error
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
        raw.information_schema.columns
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
        entity_key = 'ORDERS'
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




---------- SET target_audit_properties

       {%- set target_audit_properties = {
                                'database' : 'RAW', 
                                'schema' : 'ERP',
                                'entity' : 'ORDERS', 
                                'audit_key' :  audit_response[0],
                                'cdc_target' : audit_response[1],
                                'lowest_cdc' : audit_response[2],
                                'highest_cdc' : audit_response[3],
                                'cdc_data_type' : audit_response[4], 
                                'record_identifier' : 'id' } -%}
                    

---------- SCREEN VARIABLES
---- create a named variable for each screen you want to apply to the source table
---- available screens (see /macros/screens/<screen_name> for marcro profile:
---
---- COLUMN SCREENS
----    - not_null 
----    - unique
----    - accepted_range
----    - accepted_lenght
----    - accepted_values
----    - matches_pattern
----    - excluded_values
----
---- STATISITCAL SCREENS
----    - frequency_distribution
----    - row_count_range
---- 
---- BUSINESS SCREEN
---- this 'catch all' screen allows you to declare a complex WHERE clause to test against. For example, 
---- a business screen might be "Only customer records with an RFM score > 75 should be in the high-value segment."
---- In this example, pass the name of the screen 'high_value_customer_rfm_screen' and the sql_where, a statement 
----
----
---------- Business Screens
---- Business screens check record values against complex business logic.
---- For example, a business screen might be
---- "Only customer records with an RFM score > 75 should be in the high-value segment."
---- In this example, pass the name of the screen 'high_value_customer_rfm_screen' and the sql_where, a statement
---- WHERE clause that returns > 0 results on failure.

---------- one line per screen   
    {% set placed_by_administrator_id_not_null = {'column':'PLACED_BY_ADMINISTRATOR_ID','type':'not_null'} %} 



---------- COLLECT VARIABLES
---- add each screen variable above to the collection
    {% set screen_collection =  [
                                    placed_by_administrator_id_not_null
                                ]%}

    {{screen_partial(screen_collection, target_audit_properties)}}


{% else %}

---- when no new data is present, return an empty table
    SELECT
        *
    FROM
        {{this.database}}.{{this.schema}}.error_event_fact
    WHERE 1=0
{% endif %}



{#---------- MODEL CONFIGURATION #}
{{config({

    "materialized" : "view",
    "schema" : "QUALITY",
    "alias" : this.table + "_TEMP"
})}}
