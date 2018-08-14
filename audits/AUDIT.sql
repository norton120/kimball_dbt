---------- INITIAL AUDIT
---- All the initial audit runs are performed here for a build.
---- These runs are only to set the min and max values for the audit
---- and to populate the AUDIT table with a working row to update
---- as the screens complete.

---------- FORMATTING
---- To help keep this from becoming a mess, follow these rules:
---- * Keep all_audit_partial variables in alphabetical order. Yes it makes git diffs harder to read.




---- Set already exists flag
    {% set audit_exists = adapter.already_exists(this.schema,this.name) %}

WITH


---- Set the variables for each source to be audited.
---- variable syntax is:
---- variable_name = [<schema>, <entity>, <cdc_column>, <cdc_column_data_type>,<incremental>, <entity_type>, <database>]
---- see the macro definition for more info at /macros/kdbt_utils/initial_audit_partial.sql

    {% set erp_fiscal_calendars  = ["ERP", "FISCAL_CALENDARS","XMIN__TEXT__BIGINT","NUMBER"] %}
    {% set erp_products  = ["ERP", "PRODUCTS","XMIN__TEXT__BIGINT","NUMBER"] %}
    {% set erp_users  = ["ERP", "DW_USERS_VIEW","XMIN","NUMBER"] %}




---- combine the lists here. This is because jinja doesn't like nested list assignment.
    {%- set all_audit_partials = [
                                erp_fiscal_calendars,
                                erp_products,
                                erp_users
                                ] -%}

---- Each macro is a self-contained CTE.
    {% for audit_partial in all_audit_partials %}

        {{initial_audit_partial(audit_partial[0], audit_partial[1], audit_partial[2], audit_partial[3])}},

    {% endfor %}

union_all_initial_audits AS (
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
                    database_key,
                    '{{var("release")}}' AS release,
                    '{{var("dbt_version")}}' AS dbt_version,
                    '{{var("app_version")}}' AS app_version,
                    lowest_cdc,
                    highest_cdc
                FROM
                    {{audit_partial[0]|lower}}_{{audit_partial[1]|lower}}_new_audit_record
            )
                {{ 'UNION' if not loop.last }}

        {% endfor %}
)

SELECT
    *
FROM
    union_all_initial_audits
WHERE
    lowest_cdc IS NOT NULL
AND
    highest_cdc IS NOT NULL



---------- CONFIGURATION
    {{config({
        "materialized":"incremental",
        "sql_where":"TRUE",
        "schema":"QUALITY",
        "post-hook":[

            "{{comment({'definition':'Every time we execute a quality process job against new data in the Data Lake the instance of that execution is called an audit.',
                        'grain':'One row per audit executed on a unique entity'})}}",

            "{{comment({'column':'audit_key','definition':'The supernatural key for the audit table.', 'scd_type' : 1})}}",
            "{{add_constraints(['Pkey', 'Null'],this.schema,'AUDIT', 'AUDIT_KEY', None, None, 'incremental')}}",

            "{{comment({'column':'ENTITY_KEY','definition' : 'The entity name (table or view) to be audited. Could be FK to information_schema, but at this time not enforced.', 'scd_type' : 1})}}",
            "{{add_constraints(['Null'],this.schema,'AUDIT', 'ENTITY_KEY', None, None, 'incremental')}}",

            "{{comment({'column':'CDC_TARGET','definition':'The change data capture column - ie the column that tells us a record has been added or updated.', 'scd_type' : 1})}}",
            "{{add_constraints(['Null'],this.schema,'AUDIT', 'CDC_TARGET', None, None, 'incremental')}}",

            "{{comment({'column':'DATABASE_KEY','definition':'The database name for the entity to be audited. Could be FK to information_schema, but at this time not enforced.', 'scd_type' : 1})}}",
            "{{add_constraints(['Null'],this.schema,'AUDIT', 'DATABASE_KEY', None, None, 'incremental')}}",

            "{{comment({'column':'DBT_VERSION','definition':'The version number of the dbt framework running when this audit was generated.', 'scd_type' : 1})}}",
            "{{add_constraints(['Null'],this.schema,'AUDIT', 'DBT_VERSION', None, None, 'incremental')}}",

            "{{comment({'column':'HIGHEST_CDC','definition':'The greatest value of the cdc_target column to be included in this audit.', 'scd_type' : 1})}}",
            "{{add_constraints(['Null'],this.schema,'AUDIT', 'HIGHEST_CDC', None, None, 'incremental')}}",

            "{{comment({'column':'ENTITY_TYPE','definition':'The schema object type of the entity to be audited. Typically a View or a Table.', 'scd_type' : 1})}}",
            "{{add_constraints(['Null'],this.schema,'AUDIT', 'ENTITY_TYPE', None, None, 'incremental')}}",

            "{{comment({'column':'APP_VERSION','definition':'The internal version number for our application.', 'scd_type' : 1})}}",
            "{{add_constraints(['Null'],this.schema,'AUDIT', 'APP_VERSION', None, None, 'incremental')}}",

            "{{comment({'column':'RELEASE','definition':'The release name that this version belongs to.', 'scd_type' : 1})}}",
            "{{add_constraints(['Null'],this.schema,'AUDIT', 'RELEASE', None, None, 'incremental')}}",

            "{{comment({'column':'SCHEMA_KEY','definition':'The schema name for the entity to be audited. Could be FK to the information_schema, but at this time not enforced.', 'scd_type' : 1})}}",
            "{{add_constraints(['Null'],this.schema,'AUDIT', 'SCHEMA_KEY', None, None, 'incremental')}}",

            "{{comment({'column':'AUDIT_STATUS','definition':'Possible values are In Process and Completed. Only In Process while the screens are being applied and error events are generated.', 'scd_type' : 1})}}",
            "{{add_constraints(['Null'],this.schema,'AUDIT', 'AUDIT_STATUS', None, None, 'incremental')}}",

            "{{comment({'column':'LOWEST_CDC','definition':'The least value of the cdc_target column that will be included in this audit.', 'scd_type' : 1})}}",
            "{{add_constraints(['Null'],this.schema,'AUDIT', 'LOWEST_CDC', None, None, 'incremental')}}"




   ]})}}

