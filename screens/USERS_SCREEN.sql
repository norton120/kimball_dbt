---------- USERS_SCREEN SCREEN
----
---- Screens are source-data-quality tests that we use to investigate and record data quality.     
----
----
----
---------- Statement for establishing target_audit
----
---- The target audit data is applied to each screen. Set it in Jinja context here.

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
            'RAW'.information_schema.columns
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
        ORDER BY audit_key DESC 
        LIMIT 1

    {%- endcall -%}
    {%- set audit_response = load_result('target_audit')['data'][0] -%}

-- update the record identifier to match the table primary key

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
                    

----
---- 
---- Screens accept 2 arguments: a dict with the target path, and a 2nd dict with keys for their respective properties.
----
----
---- All screens require an exception_action key with one of 4 values:
---- - Ignore : pass the record without action, but record the error
---- - Flag : pass the record but flag it as a quality issue 
---- - Reject : discard the record, record the error
---- - Halt : stop ETL process and sound alarm
---- Default value is Flag


---------- Column Property Screens


---- Column property screens check each record for questionable values.
---- Available screens:
---- 
----    - null_screen({'column':'<column_name>'}, target_audit_properties)
----    - accepted_range_screen({'column':'<column_name>','min':'<min_value>','max':'<max_value>'})
----    - accepted_length_screen({'column':'<column_name>','min_length':'<min_length_value>','max_length':'<max_length_value>'})
----    - accepted_values_screen()
----    - pattern_screen()
----    - blacklist_screen()
----
---------- Structure Screens
---- Structure screens check relationships between columns and tables.
---- Available screens:
----
----    - foreign_key_screen()
----    - parent_child_screen()
----
----
----
----
---------- Business Screens
---- Business screens check record values against complex business logic.
---- For example, a business screen might be 
---- "Only customer records with an RFM score > 75 should be in the high-value segment."
---- In this example, pass the name of the screen 'high_value_customer_rfm_screen' and the sql_where, a statement 
---- WHERE clause that returns > 0 results on failure.
----
---- Example:
---- business_screen({'name':'high_value_rfm_screen', 'sql_where':' "segment = \'High Value\' AND rfm_score < 100"})
----
----

---------- MODEL CONFIGURATION
{{config({

    "materialized":"ephemerial",
    "sql_where":"TRUE",
    "schema":"QUALITY"

})}}
    
