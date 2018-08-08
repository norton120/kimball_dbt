{#---------- DATE PRODUCTION ENTITY
---- 
---- production models generate the final consumer-ready production data. In this layer we manage 2 key aspects: 
---- - Slowly Changing Dimensions. This model supports types 0, 1 and 2 at this time.
----        
---- - Transforms. This is where all your actual transforms on the source data belong, 
----    preferably abstracted to partials if they get unweildy here.
----
----    Steps to production:
----    1) Define your final production table schema inside the model_definition object. 
----        all columns must be type 0,1, or 2. The record identifier is the persistant id for the row. 
----
----    2) Define all your transforms inside the staging_quality.transformed CTE. The final statement inside
----        staging_quality should be identical to your production structure. 
----    3) Invoke the finalize_scd macro in the post hook to expire and update dimension rows
----    4) define constraints and comments in post hooks
---- 
#}

{#---- Declare the final production columns here in the 3 lists: #}
{% set model_definition = {'this' : this.database + '.' + this.schema + '.' + this.name,  
                           'name' : this.name, 
                            'target_exists' : adapter.already_exists(this.schema, this.name), 
                            'record_identifier' : 'date_id', 
                            'type_2_cols' : [
                                            'full_date_description',
                                            'day_of_week',
                                            'day_number_of_week',
                                            'day_number_of_month',
                                            'day_number_in_calendar_year',
                                            'day_number_in_fiscal_year',
                                            'last_day_of_month_indicator',
                                            'week_end_date_key',
                                            'week_start_date_key',
                                            'calendar_week',
                                            'calendar_month_name',
                                            'calendar_month_number_in_year',
                                            'calendar_year_month',
                                            'calendar_year',
                                            'fiscal_week',
                                            'fiscal_quarter',
                                            'fiscal_year_quarter',
                                            'first_day_fiscal_period_indicator',
                                            'last_day_fiscal_period_indicator',
                                            'holiday',
                                            'weekday_indicator',
                                            'fiscal_year_period_week'
                                               
                                            ],
                            'type_1_cols' : [],
                            'type_0_cols' : [] } %}

WITH
{#---- staging_quality rows from the newest audit.#}
staging_quality AS (
    WITH
    untransformed AS (
        SELECT

            {#-- enumerate the needed source data columns#}

        FROM
            {{this.database}}.{{this.schema | replace('GENERAL','STAGING_QUALITY')}}.FISCAL_CALENDARS_STAGING_QUALITY
        WHERE 
            audit_key = (SELECT 
                            MAX(audit_key) 
                        FROM 
                        {{this.database}}.{{this.schema | replace('GENERAL','STAGING_QUALITY')}}.FISCAL_CALENDARS_STAGING_QUALITY)
    ),
    transformed AS (
        {#-- transforms happen here to conform with the production table.#}
        {#-- this is done before we run the scd engine.#}
        SELECT * FROM untransformed
    )
    
    SELECT
        *
    FROM
        transformed

),

{{scd_engine('staging_qualiy', model_definition)}}


{# --add constraint and comment macros as needed in post-hook list #}
{{config({
    'materialized' : 'table',
    'sql_where' : 'TRUE',
    'schema' : 'GENERAL',
    'pre-hook' : "USE SCHEMA {{this.schema}}; CREATE SEQUENCE IF NOT EXISTS date_pk_seq start = 100000",
    'post-hook': [  
                    "{{comment({'column' : 'date_key', 'description' : 'Unlike normally generated surrogate keys, the date_key is the integer representation of the date - so 2018-01-01 becomes 20180101. this is based on the (probably naive) assumption that our fiscal calendar will never change historically. If / when it does, we will need to append a version number to the future keys - so 2018-01-01 would become 220180101 for version 2.' })}}"  

                ]
        
                

})}}

{#---- DEPENDENCY HACK #}
---- {{ref('FISCAL_CALENDARS_STAGING_QUALITY')}}



