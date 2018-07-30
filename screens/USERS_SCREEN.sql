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
---- All screens are flag unless noted

---------- AGE_RANGE
---- valid values are [4,3,2,7,5 and NULL]
{% set age_range_valid_values = {'column':'AGE_RANGE', 'type' : 'valid_values','valid_values' : [4,3,2,7,5], 'allow_null' : True, 'value_type' : 'number'} %}
---- all users created > 2015-02-01 are null. Older users are still updated daily due to facebook.
{% set age_range_created_before_2015 = {'column':'AGE_RANGE', 'type' : 'custom', 
    'sql_where' : "age_range IS NOT NULL and created_at > '2015-02-01'", 'screen_name' : 'age_range_created_before_2015'} %}

--------- EMAIL_ADDRESS
---- must only be null if user is_anaonymous
{% set email_only_null_for_anon = {'column':'email_address','type' : 'custom', 'sql_where' : 'email_address IS NULL AND NOT is_anonymous','screen_name':'email_only_null_for_anon'} %}
---- must be in the format <string>@<string> or NULL
---- flag for the email 'robaan@web.com'. this single user has 134k accounts.

---------- FIRST_NAME
---- Must be only alphabetical characters
---- Must not equal 'revzilla' 
---- Must be > 1 character

---------- SEGMENT_MASK
---- valid range is 0-511
---- TODO: what is this? How is this defined? 

---------- _METADATA__UUID
----
---------- LAST_LOGIN_AT
----
---------- IS_DELETED
----
---------- PASSWORD_RESET_REQUESTED_AT
----
---------- IS_ACTIVE
----
---------- ROLE_ID
----
---------- GENDER
----
---------- ID_HASH_KEY
----
---------- LAST_NAME
----
---------- XMIN
----
---------- ID
----
---------- IS_FRAUD
----
---------- TRANSPARENT_SIGNUP
----
---------- _METADATA_CONSOLIDATION
----
---------- PROFILE_IMAGE
----
---------- DEALER_ID
----
---------- ESP_ID
----
---------- _METADATA__TIMESTAMP
----
---------- UPDATED_AT
----
---------- IS_FRAUD_VERIFIED
----
---------- BRAINTREE_CUSTOMER_ID
----
---------- COUNTRY_OF_RESIDENCE
----
---------- CREATED_AT
----
---------- IS_ANONYMOUS
----
---------- PERMISSION_SECTION_GROUP_ID
----
---------- SEND_REVIEW_FOLLOWUP
----
---------- BIRTH_DATE
----
---------- DEPARTMENT_ID
----
---------- SITE_ID
----
---------- XMIN__TEXT__BIGINT


    {% set screen_collection =  [
                                    age_range_created_before_2015,
                                    age_range_valid_values

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
    
