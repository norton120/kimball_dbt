---------- INITIAL AUDIT 
---- All the initial audit runs are performed here for a build.
---- These runs are only to set the min and max values for the audit 
---- and to populate the AUDIT table with a working row to update
---- as the screens complete. 

---------- FORMATTING
---- To help keep this from becoming a mess, follow these rules: 
---- * 3 newlines between CTEs
---- * Keep initial_audit_partial macros in alphabetical order. Yes it makes git diffs harder to read.


---------- CONFIGURATION
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


---- Set the variables for each source to be audited. 
---- variable syntax is:
---- variable_name = [<schema>, <entity>, <cdc_column>, <cdc_column_data_type>,<incremental>, <entity_type>, <database>]
---- see the macro definition for more info at /macros/kdbt_utils/initial_audit_partial.sql

    {% set erp_orders = ["ERP", "ORDERS","_METADATA__TIMESTAMP","TIMESTAMP_NTZ"] %}
    {% set erp_users  = ["ERP", "DW_USERS_VIEW","_METADATA__TIMESTAMP","TIMESTAMP_NTZ"] %}



---- combine the lists here. This is because jinja doesn't like nested list asssignment.
    {%- set all_audit_partials = [
                                erp_orders,
                                erp_users
                                ] -%}

---- Each macro is a self-contained CTE.
    {% for audit_partial in all_audit_partials %}

        {{initial_audit_partial(audit_partial[0], audit_partial[1], audit_partial[2], audit_partial[3])}} 
        
        {{ ',' if not loop.last }}

    {% endfor %}

SELECT
    *
FROM

---- unions all the CTEs into one big audit set
    {% for audit_partial in all_audit_partials %}
        (
            SELECT
                audit_key,
                audit_status,
                cdc_target,
                entity_type,
                entity_key,
                schema_key,
                database_Key,
                dbt_version,
                dbt_repo_release_version,
                lowest_cdc,
                highest_cdc 
            FROM
                {{audit_partial[0]|lower}}_{{audit_partial[1]|lower}}_new_audit_record
        )
            {{ 'UNION' if not loop.last }}
        
    {% endfor %}


    
