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
staging_quality AS (
    SELECT
        *
    FROM
        {{this.database}}.{{this.schema | replace('GENERAL','STAGING_QUALITY')}}.ERP_FISCAL_CALENDARS
),



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
        staging_quality
    WHERE
        year_start_date IS NOT NULL
),



fiscal_period_last_days AS (
    SELECT
        {{date_key('DATEADD(day,-1,"DATE")')}} AS date_key,
        is_first_day_fiscal_period AS is_last_day_fiscal_period
    FROM
        staging_quality
),


---- we need to account for the leap week days, so we do that by making them fractional values of a day.
day_of_fiscal_year AS (
    SELECT
        {{date_key("DATE")}} AS date_key,
        
        CASE
            WHEN contains_leap_week THEN
                CASE
                    WHEN fiscal_week = 0 THEN DATEDIFF('days', year_start_date, DATEADD('days',(-1 * week_day_number), "DATE"))
                                                        + (0.1 * week_day_number) + 1
                    WHEN fiscal_week BETWEEN 1 AND 5 THEN DATEDIFF('day',year_start_date,"DATE") + 1
                    ELSE DATEDIFF('day',year_start_date,"DATE") - 6
                END
            ELSE DATEDIFF('day',year_start_date,"DATE") + 1 
        END AS day_number_in_fiscal_year
    FROM
        staging_quality
    LEFT JOIN
        fiscal_year_first_dates
    ON
        staging_quality.fiscal_year = fiscal_year_first_dates.fiscal_year
    LEFT JOIN
    --- this is gross. but the sf engine borks when we subquery in the case statement
        (SELECT
            {{date_key("DATE")}} AS date_key,
            TRUE AS contains_leap_week
        FROM
            staging_quality
        WHERE
            fiscal_year IN (SELECT
                                DISTINCT fiscal_year
                            FROM 
                                staging_quality
                            WHERE
                                fiscal_week = 0) )leap_week
    ON leap_week.date_key = {{date_key('staging_quality."DATE"')}}
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
    day_number_in_fiscal_year,

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

    CASE
        when fiscal_week = 0 THEN 5.5
        ELSE fiscal_week
    END AS fiscal_week,

    fiscal_qtr AS fiscal_quarter,
    staging_quality.fiscal_year,
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
    staging_quality
LEFT JOIN
    fiscal_year_first_dates
ON
    fiscal_year_first_dates.fiscal_year = staging_quality.fiscal_year
LEFT JOIN
    fiscal_period_last_days 
ON
    fiscal_period_last_days.date_key = {{date_key('staging_quality."DATE"')}}
LEFT JOIN
    day_of_fiscal_year 
ON
    day_of_fiscal_year.date_key = {{date_key('staging_quality."DATE"')}}

 
{# --add constraint and comment macros as needed in post-hook list #}
{{config({
    'materialized' : 'table',
    'schema' : 'GENERAL',
    'pre-hook' : "USE SCHEMA {{this.schema}};",
    'post-hook': [  

                    "{{comment({'description' : 'The dimension for all dates in the data warehouse. Note: This table will never be directly related to by another entity, but instead aliased by prefixed views.', 'grain' : 'one instance per calendar day.' })}}",

                    "{{comment({'column' : 'date_key', 'description' : 'PK defined as the integer representation of the date. For example, 2018-01-01 becomes 20180101. 0 represents Data Not Applicable, 99991231 represents Data Not Yet Available.' })}}",
                    "{{add_constraints(['Pkey','Null'], this.schema, 'DATE', 'date_key')}}",
                    
                    "{{comment({'column' : 'full_date_description', 'description' : 'The common English representation of a date, ie January 1, 1979.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null','Unique'], this.schema, 'DATE', 'full_date_description')}}"

                    "{{comment({'column' : 'fiscal_week', 'description' : 'The week of the fiscal year. Note that there is an extra leap week every 7 years, which is placed between week 5 and week 6. This is labeled week 5.5.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'fiscal_week')}}",


                    "{{comment({'column' : 'LAST_DAY_FISCAL_PERIOD_INDICATOR', 'description' :  'Options are Period Last Day and Not Period Last Day.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'LAST_DAY_FISCAL_PERIOD_INDICATOR')}}",

                    "{{comment({'column' : 'LAST_DAY_OF_MONTH_INDICATOR', 'description' :'Options are Month End and Not Month End.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'LAST_DAY_OF_MONTH_INDICATOR')}}",

                    "{{comment({'column' : 'WEEKDAY_INDICATOR', 'description' : 'Options are Weekday and Weekend.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'WEEKDAY_INDICATOR')}}",

                    "{{comment({'column' : 'WEEK_END_DATE_KEY', 'description' : 'The date_key for the day ending the week the subject date belongs to.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Fkey','Null'], this.schema, 'DATE', 'WEEK_END_DATE_KEY', 'DATE', 'DATE_KEY')}}",

                    "{{comment({'column' : 'DATE', 'description' : 'The date-typed column containing the representation of the date as a date object.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null', 'Unique'], this.schema, 'DATE', 'DATE')}}",

                    "{{comment({'column' : 'FISCAL_YEAR_QUARTER', 'description' : 'A string formatted as <year>-<quarter>.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'FISCAL_YEAR_QUARTER')}}",

                    "{{comment({'column' : 'CALENDAR_MONTH_NUMBER_IN_YEAR', 'description' : 'The integer value of the month 1-12.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'CALENDAR_MONTH_NUMBER_IN_YEAR')}}",

                    "{{comment({'column' : 'DAY_OF_WEEK', 'description' : 'Conformed day of the week Monday-Sunday for the given date.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'DAY_OF_WEEK')}}",

                    "{{comment({'column' : 'WEEK_START_DATE_KEY', 'description' : 'The date_key for the day ending the week the subject date belongs to.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Fkey','Null'], this.schema, 'DATE', 'WEEK_START_DATE_KEY', 'DATE', 'DATE_KEY')}}",

                    "{{comment({'column' : 'FISCAL_QUARTER', 'description' : 'The fiscal quarter 1-4 for the given date.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'FISCAL_QUARTER')}}",

                    "{{comment({'column' : 'CALENDAR_YEAR', 'description' : 'The Gregorian calendar year representation of the subject date.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'CALENDAR_YEAR')}}",

                    "{{comment({'column' : 'CALENDAR_MONTH_NAME', 'description' : 'The Engish language name of the month representation of the subject date.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'CALENDAR_MONTH_NAME')}}",

                    "{{comment({'column' : 'CALENDAR_YEAR_MONTH', 'description' : 'A string in the format <year>-<month> with numbers, ie 2018-1.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'CALENDAR_YEAR_MONTH')}}",

                    "{{comment({'column' : 'DAY_NUMBER_IN_FISCAL_YEAR', 'description' : 'Number of days from the first day of the given fiscal year as integer, for normal years this will range 1-364. For leap week (every 7 years), this will have a decimal value with .1 for each day of the leap week. If the Sunday before leap week is day_number_in_fiscal_year 35, Monday will be 35.1, Tuesday 35.2 etc.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'DAY_NUMBER_IN_FISCAL_YEAR')}}",

                    "{{comment({'column' : 'DAY_NUMBER_IN_WEEK', 'description' : 'Integer representation of the week day, 1-7 starting on Monday.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'DAY_NUMBER_IN_WEEK')}}",

                    "{{comment({'column' : 'FIRST_DAY_FISCAL_PERIOD_INDICATOR', 'description' : 'Options are Period First Day, Not Period First Day.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'FIRST_DAY_FISCAL_PERIOD_INDICATOR')}}",

                    "{{comment({'column' : 'HOLIDAY', 'description' : 'The textual representation of the holiday for the given date, or Not Holiday.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'HOLIDAY')}}",

                    "{{comment({'column' : 'DAY_NUMBER_IN_MONTH', 'description' : 'The integer value day number in the Gregorian month (matches the normal calendar month) 1-31.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'DAY_NUMBER_IN_MONTH')}}",

                    "{{comment({'column' : 'FISCAL_YEAR', 'description' : 'The integer value of the fiscal year.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'FISCAL_YEAR')}}",

                    "{{comment({'column' : 'DAY_NUMBER_IN_CALENDAR_YEAR', 'description' : 'The count of days from the start of the Gregorian year. Does not take into account fiscal leap weeks.', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'DAY_NUMBER_IN_CALENDAR_YEAR')}}",

                    "{{comment({'column' : 'FISCAL_YEAR_PERIOD_WEEK', 'description' : 'Textual representation in the format <year>-<period>-<week>. ', 'scd_type' : 1 })}}",
                    "{{add_constraints(['Null'], this.schema, 'DATE', 'FISCAL_YEAR_PERIOD_WEEK')}}"

                ]
        
                

})}}

{#---- DEPENDENCY HACK #}
---- {{ref('FISCAL_CALENDARS_STAGING_QUALITY')}}



