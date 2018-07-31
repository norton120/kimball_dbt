---------- PRODUCTS_SCREEN SCREEN

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
            entity_key = 'PRODUCTS'
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
                                'entity' : 'PRODUCTS',
                                'audit_key' :  audit_response[0],
                                'cdc_target' : audit_response[1],
                                'lowest_cdc' : audit_response[2],
                                'highest_cdc' : audit_response[3],
                                'cdc_data_type' : audit_response[4],
                                'record_identifier' : 'id' } -%}


---------- SCREEN VARIABLES
---- create a named variable for each screen you want to apply to the source table
---- available screens (see /macros/screens/<screen_name> for macro profile:
----
---- COLUMN SCREENS
---------- id (bigint)
----    - not_null
    {% set id_not_null = {'column':'id', 'type':'null_screen'} %}
----    - unique
    {% set id_is_unique = {'column':'id', 'type':'unique_screen'} %}
----    - values_at_least (1)
    {% set id_at_least_one = {'column':'id', 'type':'values_at_least', 'provided_value':'1'} %}

---------- additional_shipping_charge (numeric)
---- (should not have values less than 0 (zero))
----    - values_at_least (0)
    {% set additional_shipping_charge_at_least_zero = {'column':'additional_shipping_charge', 'type':'values_at_least', 'provided_value':'0'} %}

---------- allow_closeout_exchange (boolean)
----

---------- allow_discounting (boolean)
----

---------- apparel_material_mask (integer)
----    - values_at_least (0)
    {% set apparel_material_mask_at_least_zero = {'column':'apparel_material_mask', 'type':'values_at_least', 'provided_value':'0'} %}

---------- apparel_type (integer)
----    - values_at_least (1)
    {% set apparel_type_at_least_one = {'column':'apparel_type', 'type':'values_at_least', 'provided_value':'1'} %}

---------- application_imports (text)
----

---------- apply_sale_price (boolean)
----

---------- auto_generate_teaser (boolean)
----

---------- blemish_caption (text)
----

---------- blemish_notes (text)
----

---------- brand_id (integer)
----    - not_null
    {% set brand_id_not_null = {'column':'brand_id', 'type':'null_screen'} %}
----    - values_at_least (1)
    {% set brand_id_at_least_one = {'column':'brand_id', 'type':'values_at_least', 'provided_value':'1'} %}

---------- browser_title (character varying(400))
----

---------- closed_out_at (timestamp with time zone)
----    - date_range_within_history
    {% set closed_out_at_range_within_history = {'column':'closed_out_at', 'type':'date_range_within_history'} %}


---------- country_of_origin (character varying(3))
----    - MAX(LENGTH(country_of_origin)) = 3


---------- created_at (timestamp with time zone)
----    - date_range_within_history
    {% set created_at_range_within_history = {'column':'created_at', 'type':'date_range_within_history'} %}


---------- creator_id (bigint)
----    - values_at_least (1)
    {% set creator_id_at_least_one = {'column':'creator_id', 'type':'values_at_least', 'provided_value':'1'} %}

----------
----

----------
----

----------
----

----------
----


----    - accepted_range
----    - accepted_length
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
---- WHERE clause that returns > 0 results on failure.




---------- COLLECT VARIABLES
---- add each screen variable above to the collection
    {% set screen_collection =  [
                                    id_not_null,
                                    id_is_unique,
                                    id_at_least_one,
                                    additional_shipping_charge_at_least_zero,
                                    apparel_material_mask_at_least_zero,
                                    apparel_type_at_least_one,
                                    brand_id_not_null,
                                    brand_id_at_least_one,
                                    closed_out_at_range_within_history,
                                    created_at_range_within_history,
                                    ,
                                    ,
                                    ,
                                    ,
                                    ,
                                    ,
                                    ,

                                ]%}

---------- RUN SCREENS [leave this section alone!]
WITH
        {{screen_declaration(screen_collection, target_audit_properties)}}


---------- UNION [leave this section alone!]

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

---------- CONFIGURATION [leave this section alone!]
{{config({

    "materialized":"ephemeral",
    "sql_where":"TRUE",
    "schema":"QUALITY"

})}}
