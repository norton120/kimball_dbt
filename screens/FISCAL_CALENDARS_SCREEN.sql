{#---------- FISCAL_CALENDARS_SCREEN SCREEN
----
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
#}
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
            entity_key = 'FISCAL_CALENDARS'
        AND
            audit_status = 'In Process'
        ORDER BY audit_key DESC
        LIMIT 1

    {%- endcall -%}

{% set audit_response_data_object = load_result('target_audit')['data']%}
{#---------- END STATMENTS #}

{# ---- if there is no new data, skip the entire screen model #}
{% if audit_response_data_object | length > 0 %}

    {%- set audit_response = audit_response_data_object[0] -%}
{# -- update the record identifier to match the table primary key #}

        {%- set target_audit_properties = {
                                'database' : 'RAW',
                                'schema' : 'ERP',
                                'entity' : 'FISCAL_CALENDARS',
                                'audit_key' :  audit_response[0],
                                'cdc_target' : audit_response[1],
                                'lowest_cdc' : audit_response[2],
                                'highest_cdc' : audit_response[3],
                                'cdc_data_type' : audit_response[4],
                                'record_identifier' : 'id' } -%}

{#
---------- SCREEN VARIABLES
#}

-------- DAY_OF_WEEK
---- not null
{% set day_of_week_not_null = {'column' : 'day_of_week', 'type' : 'not_null'} %}
---- values Monday - Sunday
{% set day_of_week_valid_days = {'column' : 'day_of_week', 'type' : 'valid_values', 'value_type' : 'TEXT', 'valid_values' : ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'], 'allow_null' : False} %}

-------- FISCAL_YEAR
---- character length is 4
{% set fiscal_year_4_chars = {'column' : 'fiscal_year', 'type' : 'exact_length', 'length_value' : 4 } %}
---- not null
{% set fiscal_year_not_null = {'column' : 'fiscal_year', 'type' : 'not_null'} %}

-------- HOL_IND
---- must not be null if holiday is null, and must be null on non-holidays
{% set hol_ind_matches_holiday = {'column' : 'hol_ind', 'type' : 'custom', 'screen_name' : 'hol_ind_matches_holiday', 'sql_where' : '(hol_ind = 0) <> (holiday IS NULL)' } %}
---- 1 = NewYrs, 2 = GdFri, 3 = Easter, 4 = Mem, 5 = Indep, 6 = Lab, 7 = ThanksG, 8 = BlkFri, 9 = CyMon, 10 = Chr
{% set hol_ind_on_correct_holidays = {'column' : 'hol_ind', 'type' : 'custom', 'screen_name' : 'hol_ind_correct_days', 'sql_where' :
"(hol_ind = 1 AND holiday <> 'NewYrs')
 OR
 (hol_ind = 2 AND holiday <> 'GdFri')
 OR
 (hol_ind = 3 AND holiday <> 'Easter')
 OR
 (hol_ind = 4 AND holiday <> 'Mem')
 OR
 (hol_ind = 5 AND holiday <> 'Indep')
 OR
 (hol_ind = 6 AND holiday <> 'Lab')
 OR
 (hol_ind = 7 AND holiday <> 'ThanksG')
 OR
 (hol_ind = 8 AND holiday <> 'BlkFri')
 OR
 (hol_ind = 9 AND holiday <> 'CyMon')
 OR
 (hol_ind = 10 AND holiday <> 'Chr')" } %}
--- every year must have one and only one of each holiday
{% set one_and_only_one_of_each_holiday_a_year = {'column' : 'hol_ind', 'type' : 'custom_aggregate', 'screen_name' : 'one_and_only_one_of_each_holiday_a_year', 'sql_where' : 'fiscal_year IN (SELECT fiscal_year FROM (SELECT fiscal_year, SUM(hol_ind) hol_total FROM raw.erp.fiscal_calendars GROUP BY 1 HAVING hol_total <> 55))' } %}

-------- WEEK_DAY_NUMBER
---- range 1-7
{% set week_day_number_range = {'column' : 'week_day_number', 'type' : 'range', 'range_start' : 1, 'range_end' : 7, 'cast_as' : 'NUMBER' } %}
---- Monday = 1, Sunday = 7 etc.
{% set week_day_number_correct_day = {'column' : 'week_day_number', 'type' : 'custom', 'screen_name' : 'week_day_number_correct_day', 'sql_where' :
"((week_day_number = 1 AND day_of_week <> 'Monday')
 OR
 (week_day_number = 2 AND day_of_week <> 'Tuesday')
 OR
 (week_day_number = 3 AND day_of_week <> 'Wednesday')
 OR
 (week_day_number = 4 AND day_of_week <> 'Thursday')
 OR
 (week_day_number = 5 AND day_of_week <> 'Friday')
 OR
 (week_day_number = 6 AND day_of_week <> 'Saturday')
 OR
 (week_day_number = 7 AND day_of_week <> 'Sunday'))"} %}
---- not null
{% set week_day_number_not_null = {'column' : 'week_day_number', 'type' : 'not_null'} %}

-------- HOLIDAY
---- accepted values Mem, Easter, NewYrs, CyMon, GdFri, Lab, Chr, ThanksG, Indep, BlkFri
{% set holiday_values = {'column' : 'holiday', 'type' : 'valid_values', 'value_type' : 'TEXT', 'valid_values' : ['Mem', 'Easter', 'NewYrs', 'CyMon', 'GdFri', 'Lab', 'Chr', 'ThanksG', 'Indep', 'BlkFri'], 'allow_null' : False} %}
---- Easter is always a Sunday
{% set easter_is_sunday = {'column' : 'holiday', 'type' : 'association', 'column_value' : 'Easter', 'depending_column' : 'day_of_week', 'depending_value' : 'Sunday', 'depending_data_type' : 'string', 'column_data_type' : 'string'} %}
---- Thanksgiving is always a Thursday
{% set thanksgiving_is_sunday = {'column' : 'holiday', 'type' : 'association', 'column_value' : 'ThanksG', 'depending_column' : 'day_of_week', 'depending_value' : 'Thursday', 'depending_data_type' : 'string', 'column_data_type' : 'string'} %}
---- Christmas is always in December
{% set christmas_in_december = {'column' : 'holiday', 'type' : 'association', 'column_value' : 'Chr', 'depending_column' : 'iso_month', 'depending_value' : 12, 'depending_data_type' : 'integer', 'column_data_type' : 'string'} %}
---- New Years is always in January
{% set new_years_in_january = {'column' : 'holiday', 'type' : 'association', 'column_value' : 'NewYrs', 'depending_column' : 'iso_month', 'depending_value' : 1, 'depending_data_type' : 'integer', 'column_data_type' : 'string'} %}
---- Memorial Day is always in May
{% set memorial_day_in_may = {'column' : 'holiday', 'type' : 'association', 'column_value' : 'Mem', 'depending_column' : 'iso_month', 'depending_value' : 5, 'depending_data_type' : 'integer', 'column_data_type' : 'string'} %}
---- Independence Day is always in July
{% set independence_day_in_july = {'column' : 'holiday', 'type' : 'association', 'column_value' : 'Indep', 'depending_column' : 'iso_month', 'depending_value' : 7, 'depending_data_type' : 'integer', 'column_data_type' : 'string'} %}
---- Labor Day is always in September
{% set labor_day_in_september = {'column' : 'holiday', 'type' : 'association', 'column_value' : 'Lab', 'depending_column' : 'iso_month', 'depending_value' : 9, 'depending_data_type' : 'integer', 'column_data_type' : 'string'} %}
---- Thanksgiving is always in November
{% set thanksgiving_in_november = {'column' : 'holiday', 'type' : 'association', 'column_value' : 'ThanksG', 'depending_column' : 'iso_month', 'depending_value' : 11, 'depending_data_type' : 'integer', 'column_data_type' : 'string'} %}

-------- ISO_QTR
---- range 1-4
{% set iso_qtr_range = {'column' : 'iso_qtr', 'type' : 'range', 'range_start' : 1, 'range_end' : 4, 'cast_as' : 'NUMBER' } %}
---- not null
{% set iso_qtr_not_null = {'column' : 'iso_qtr', 'type' : 'not_null'} %}

-------- FISCAL_WEEK
---- range 0-52
{% set fiscal_week_range = {'column' : 'fiscal_week', 'type' : 'range', 'range_start' : 0, 'range_end' : 52, 'cast_as' : 'NUMBER' } %}
---- not null
{% set fiscal_week_not_null = {'column' : 'fiscal_week', 'type' : 'not_null'} %}

-------- ISO_WEEK
---- range 1-53
{% set iso_week_range = {'column' : 'iso_week', 'type' : 'range', 'range_start' : 1, 'range_end' : 53, 'cast_as' : 'NUMBER' } %}
---- not null
{% set iso_week_not_null = {'column' : 'iso_week', 'type' : 'not_null'} %}

-------- ISO_YEAR
---- character length 4
{% set iso_year_4_chars = {'column' : 'iso_year', 'type' : 'exact_length', 'length_value' : 4 } %}
---- not null
{% set iso_year_not_null = {'column' : 'iso_year', 'type' : 'not_null'} %}

-------- IS_FIRST_DAY_FISCAL_PERIOD
---- not null
{% set is_first_day_fiscal_period_not_null = {'column' : 'is_first_day_fiscal_period', 'type' : 'not_null'} %}
---- should be 12 of these a year
{% set is_first_day_fiscal_period_12_per_year = {'column' : 'is_first_day_fiscal_period', 'screen_name' : 'is_first_day_fiscal_period_12_per_year', 'type' : 'custom_aggregate', 'sql_where' : 'fiscal_year IN (SELECT fiscal_year FROM (SELECT fiscal_year, COUNT(*) countstar FROM raw.erp.fiscal_calendars WHERE is_first_day_fiscal_period GROUP BY 1 HAVING countstar <> 12) )'} %}

-------- FISCAL_PERIOD
---- range 1-12
{% set fiscal_period_range = {'column' : 'fiscal_period', 'type' : 'range', 'range_start' : 1, 'range_end' : 12, 'cast_as' : 'NUMBER' } %}
---- not null
{% set fiscal_period_not_null = {'column' : 'fiscal_period', 'type' : 'not_null'} %}

-------- FISCAL_QTR
---- range 1-4
{% set fiscal_qtr_range = {'column' : 'fiscal_qtr', 'type' : 'range', 'range_start' : 1, 'range_end' : 4, 'cast_as' : 'NUMBER' } %}
---- not null
{% set fiscal_qtr_not_null = {'column' : 'fiscal_qtr', 'type' : 'not_null'} %}

-------- ID
---- unique
{% set id_unique = {'column' : 'id', 'type' : 'unique'} %}
---- not null
{% set id_not_null = {'column' : 'id', 'type' : 'not_null'} %}

-------- DATE
---- unique
{% set date_unique = {'column' : 'date', 'type' : 'unique'} %}
---- not null
{% set date_not_null = {'column' : 'date', 'type' : 'not_null'} %}

-------- ISO_MONTH
---- range 1-12
{% set iso_month_range = {'column' : 'iso_month', 'type' : 'range', 'range_start' : 1, 'range_end' : 12, 'cast_as' : 'NUMBER' } %}
---- not null
{% set iso_month_not_null = {'column' : 'iso_month', 'type' : 'not_null'} %}


{#
---------- COLLECT VARIABLES
---- add each screen variable above to the collection
#}
    {% set screen_collection =  [
                                    christmas_in_december,
                                    date_not_null,
                                    date_unique,
                                    day_of_week_not_null,
                                    day_of_week_valid_days,
                                    easter_is_sunday,
                                    fiscal_period_range,
                                    fiscal_period_not_null,
                                    fiscal_qtr_range,
                                    fiscal_qtr_not_null,
                                    fiscal_week_range,
                                    fiscal_week_not_null,
                                    fiscal_year_4_chars,
                                    fiscal_year_not_null,
                                    hol_ind_matches_holiday,
                                    hol_ind_on_correct_holidays,
                                    holiday_values,
                                    id_not_null,
                                    id_unique,
                                    independence_day_in_july,
                                    is_first_day_fiscal_period_not_null,
                                    is_first_day_fiscal_period_12_per_year,
                                    iso_month_range,
                                    iso_month_not_null,
                                    iso_qtr_range,
                                    iso_qtr_not_null,
                                    iso_week_range,
                                    iso_week_not_null,
                                    iso_year_4_chars,
                                    iso_year_not_null,
                                    labor_day_in_september,
                                    memorial_day_in_may,
                                    new_years_in_january,
                                    one_and_only_one_of_each_holiday_a_year,
                                    thanksgiving_in_november,
                                    thanksgiving_is_sunday,
                                    week_day_number_range,
                                    week_day_number_correct_day,
                                    week_day_number_not_null
                                ]%}

{# ---------- RUN SCREENS [leave this section alone!] #}
WITH
        {{screen_declaration(screen_collection, target_audit_properties)}}


{# ---------- UNION [leave this section alone!] #}

    SELECT
        *
    FROM
        (
            {{screen_union_statement(screen_collection, target_audit_properties)}}

        )


{% else %}

{# ---- when no new data is present, return an empty table #}
    SELECT
        *
    FROM
        {{this.database}}.{{this.schema}}.error_event_fact
    WHERE 1=0
{% endif %}

{# ---------- CONFIGURATION [leave this section alone!] #}
{{config({

    "materialized":"ephemeral",
    "sql_where":"TRUE",
    "schema":"QUALITY"

})}}
