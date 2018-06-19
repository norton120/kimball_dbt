---------- INITIAL AUDIT PARTIAL 
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
        "sql_where":"TRUE"
    })}}
---- end config


---- Set already exists flag 
    {%- set audit_exists = adapter.already_exists(this.schema,this.name) -%}

WITH

---- Each macro is a self-contained CTE. Just add vars and post-commas.

---- example:
----   {{initial_audit_partial("YOUR_SCHEMA", "YOUR_TABLE","YOUR_COLUMN")}}, 






    
