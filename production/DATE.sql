{#---------- DATE PRODUCTION ENTITY
---- 
---- The DATE entity is not joined directly, but aliased with a view for each instance in a give fact / dimension table
----    with an appropriate prefix. For example, the purchase_date_key in a sales fact joins the PURCHASE_DATE view,
----    which is a view of the DATE entity with purchase_ prefixed to each attribute.
----
----    DATE is a fully type 1 dimension, and we are operating under the assumption that the organization will never need
----    to 'look back' at the history of the dimension if it changes. For example, if we suddenly decide that the
----    fiscal year begins in March, we are assuming that we will adjust all dates looking both forward and back 
----    for all time (and not retain the previous calendar values). 
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



fiscal_period_last_days AS (
    SELECT
        {{date_key('DATEADD(day,-1,"DATE")')}} AS date_key,
        is_first_day_fiscal_period AS is_last_day_fiscal_period
    FROM
        {{this.database}}.{{this.schema | replace('GENERAL','STAGING_QUALITY')}}.FISCAL_CALENDARS_STAGING_QUALITY
)



SELECT
    {{date_key("DATE")}} AS date_key,
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
        WHEN is_last_day_fiscal_period THEN 'Period Last Day'
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
LEFT JOIN
    fiscal_period_last_days 
ON
    fiscal_period_last_days.date_key = {{date_key('staging_quality."DATE"')}}


 
{# --add constraint and comment macros as needed in post-hook list #}
{{config({
    'materialized' : 'table',
    'schema' : 'GENERAL',
    'pre-hook' : "USE SCHEMA {{this.schema}};",
    'post-hook': [  

                    "{{comment({'description' : 'The dimension for all dates in the data warehouse. Note: This table will never be directly related to by another entity, but instead aliased by prefixed views.', 'grain' : 'one instance per calendar day.' })}}",

                    "{{comment({'column' : 'date_key', 'description' : 'PK defined as the integer representation of the date. For example, 2018-01-01 becomes 20180101' })}}",
                    "{{add_constraints(['Pkey','Null'], this.schema, 'DATE', 'date_key')}}",
                    
                    "{{comment({'column' : 'full_date_description', 'description' : 'The common English representation of a date, ie January 1, 1979.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null','Unique'], this.schema, 'DATE', 'full_date_description')}}"

                ]
        
                

})}}

{#---- DEPENDENCY HACK #}
---- {{ref('FISCAL_CALENDARS_STAGING_QUALITY')}}



