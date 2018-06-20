---------- INITIAL AUDIT 
---- All the initial audit runs are performed here for a build.
---- These runs are only to set the min and max values for the audit 
---- and to populate the AUDIT table with a working row to update
---- as the screens complete. 

---- FORMATTING
---- To help keep this from becoming a mess, follow these rules: 
---- * 3 newlines between CTEs
---- * Keep CTEs in alphabetical order. Yes it makes git diffs harder to read.


---- CONFIG
    {{config({
        "materialized":"incremental",
        "sql_where":"TRUE",
        "schema":"QUALITY",
        "post-hook":[
            "{{comment({'column':'audit_key','definition':'The supernatural key for the audit table.'})}}",
            "{{comment({'definition':'Every time we execute a quality process job against new data in the Data Lake the instance of that execution is called an audit.', 
                        'grain':'One row per audit executed on a unique entity'})}}"    
    
   ]})}}


---- Set already exists flag 
    {% set audit_exists = adapter.already_exists(this.schema,this.name) %}

WITH

---- Each macro is a self-contained CTE. Just add vars and post-commas.

---- example:
----   initial_audit_partial("YOUR_SCHEMA", "YOUR_TABLE","YOUR_COLUMN")  


    {{initial_audit_partial("ERP", "ORDERS","_METADATA__TIMESTAMP","TIMESTAMP_NTZ")}} 

SELECT
    *
FROM
    erp_orders_new_audit_record


    
