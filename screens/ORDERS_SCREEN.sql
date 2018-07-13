---------- ORDERS_SCREEN SCREEN
----
---- Screens are source-data-quality tests that we use to investigate and record data quality.     
---- 
---- Screens accept 2 arguments: a dict with the target path, and a 2nd dict with keys for their respective properties.
----
----
---- All screens require an exception_action key with one of 4 values:
---- - Ignore : pass the record without action, but record the error
---- - Flag : pass the record but flag it as a quality issue 
---- - Reject : discard the record, record the error
---- - Halt : stop ETL process and sound alarm
----

{% set target_path = {'database':'RAW', 'schema':'ERP','entity':'ORDERS'} %}


WITH
target_audit AS (
    SELECT
        audit_key,
        cdc_target,
        lowest_cdc,
        highest_cdc
    FROM
        {{this.database}}.quality.audit
    WHERE
        database_key = 'RAW'
    AND
        schema_key = 'ERP'
    AND
        entity_key = 'ORDERS'
    ORDER BY audit_key DESC 
    LIMIT 1
),

---------- Column Property Screens
---- Column property screens check each record for questionable values.
---- Available screens:
---- 

    {{null_screen(target_path, {'column':'administrator_id'})}}


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

---------- UNION

SELECT
    *
FROM 
    raw_erp_orders_administrator_id_not_null   


{{config({

    "materialized":"ephemeral",
    "sql_where":"TRUE",
    "schema":"QUALITY"

})}}
    
