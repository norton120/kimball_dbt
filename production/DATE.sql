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

WITH
fiscal_year_first_dates AS (
    SELECT
        fiscal_year,
        CASE
            WHEN fiscal_qtr = 1
                AND fiscal_period = 1
                AND is_first_day_fiscal_period = TRUE
            THEN "DATE"
            ELSE NULL
        END AS year_start_date
    FROM
        {{this.database}}.{{this.schema | replace('GENERAL','STAGING_QUALITY')}}.FISCAL_CALENDARS_STAGING_QUALITY
    WHERE
        year_start_date IS NOT NULL
),

SELECT
    TO_CHAR("DATE", 'yyyymmdd')::integer AS date_key,
    "DATE",

    DECODE(DATE_PART('month',"DATE"),
         1 , 'January',
         2 , 'February',
         3 , 'March',
         4 , 'April',
         5 , 'May',
         6 , 'June',
         7 , 'July',
         8 , 'August',
         9 , 'September',
         10 , 'October',
         11 , 'November',
         12 , 'December'
    ) ||' '|| DATE_PART('day',"DATE")::varchar || ', '|| DATE_PART('year',"DATE")::varchar AS full_date_description,

    day_of_week,   
    week_day_number AS day_number_in_week,
    DATE_PART('day',"DATE") AS day_number_in_month,
    DATE_PART('dayofyear',"DATE") AS day_number_in_calendar_year,
    DATEDIFF('day',year_start_date,"DATE") + 1 AS day_number_in_fiscal_year,

    CASE 
        WHEN DATEDIFF('month', "DATE", DATEADD(days,1, "DATE")) <> 0 THEN 'Month End'
        ELSE 'Not Month End'
    END AS last_day_of_month_indicator,
    
    {{date_key("DATEADD(days, (7 - DATE_PART('dayofweek', \"DATE\")), \"DATE\")")}} AS week_end_date_key,
    {{date_key("DATEADD(days, (1 - DATE_PART('dayofweek', \"DATE\")), \"DATE\")")}} AS week_start_date_key,

    DECODE(DATE_PART('month',"DATE"),
         1 , 'January',
         2 , 'February',
         3 , 'March',
         4 , 'April',
         5 , 'May',
         6 , 'June',
         7 , 'July',
         8 , 'August',
         9 , 'September',
         10 , 'October',
         11 , 'November',
         12 , 'December'
    ) AS calendar_month_name,
    
    DATE_PART('month', "DATE") AS calendar_month_number_in_year,
    DATE_PART('year', "DATE")::varchar ||'-'|| DATE_PART('month',"DATE")::varchar AS calendar_year_month,
    DATE_PART('year', "DATE") AS calendar_year,
    fiscal_week,
    fiscal_qtr AS fiscal_quarter,
    staging_quality.fiscal_year::varchar || '-' || fiscal_qtr AS fiscal_year_quarter,

    CASE
        WHEN is_first_day_fiscal_period THEN 'Period First Day'
        ELSE 'Not Period First Day'
    END AS first_day_fiscal_period_indicator,
    
    CASE
        WHEN next_day_is_first_day_fiscal_period THEN 'Period Last Day'
        ELSE 'Not Period Last Day'
    END AS last_day_fiscal_period_indicator,
    
    DECODE(hol_ind, 
            1, 'New Years Day',
            2, 'Good Friday',
            3, 'Easter', 
            4, 'Memorial Day',
            5, 'Independance Day',
            6, 'Labor Day',
            7, 'Thanksgiving',
            8, 'Black Friday',
            9, 'Cyber Monday', 
            10, 'Christmas',
            'Not Holiday'
        ) AS holiday,
    
    CASE
        WHEN week_day_number < 6 THEN 'Weekday'
        ELSE 'Weekend'
    END AS weekday_indicator,

    staging_quality.fiscal_year::varchar ||'-'|| fiscal_period::varchar ||'-'|| fiscal_week::varchar AS fiscal_year_period_week        
FROM
    {{this.database}}.{{this.schema | replace('GENERAL','STAGING_QUALITY')}}.FISCAL_CALENDARS_STAGING_QUALITY AS staging_quality
LEFT JOIN
    fiscal_year_first_dates
ON
    fiscal_year_first_dates.fiscal_year = staging_quality.fiscal_year

{# --add constraint and comment macros as needed in post-hook list #}
{{config({
    'materialized' : 'table',
    'schema' : 'GENERAL',
    'pre-hook' : "USE SCHEMA {{this.schema}};",
    'post-hook': [  
                    "{{comment({'column' : 'date_key', 'description' : '' })}}"  

                ]
        
                

})}}

{#---- DEPENDENCY HACK #}
---- {{ref('FISCAL_CALENDARS_STAGING_QUALITY')}}



