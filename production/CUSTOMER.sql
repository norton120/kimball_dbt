---------- CUSTOMER PRODUCTION ENTITY
----
----

{% set target_exists = adapter.already_exists(this.schema, this.name) %}

{% set record_identifier = 'id' %}
{% set type_0_cols = ['created_at'] %}
{% set type_1_cols = ['updated_at','last_login_at'] %}
{% set type_2_cols = ['first_name','last_name','email_address','is_anonymous'] %}

WITH
---- staging_quality rows from the newest audit.
staging_quality AS (
    WITH
    untransformed AS (
        -- enumerate the needed source data columns
        SELECT
            id,
            created_at,
            updated_at,
            last_login_at,
            first_name,
            last_name,
            email_address,
            is_anonymous
        FROM
            {{this.database}}.{{this.schema | replace('GENERAL','STAGING_QUALITY')}}.USERS_STAGING_QUALITY
        WHERE 
            audit_key = (SELECT 
                            MAX(audit_key) 
                        FROM 
                        {{this.database}}.{{this.schema | replace('GENERAL','STAGING_QUALITY')}}.USERS_STAGING_QUALITY)
    ),
    transformed AS (
        -- transforms happen here to conform with the production table.
        -- this is done before we run the scd engine.
        SELECT * FROM untransformed
    )
    
    SELECT
        *
    FROM
        transformed

),


{% if target_exists %}
---- production rows that are current. 
    production AS (
        SELECT
            {{record_identifier}},
            customer_key,
            current_row,
            {{print_columns(type_0_cols)}},
            {{print_columns(type_1_cols)}},
            {{print_columns(type_2_cols)}},
            effective_date,
            expiration_date
        FROM
            {{this}}
        WHERE
            current_row        

    ),


---- delta rows 
    delta AS (
        SELECT
            *
        FROM (
            SELECT
                {{record_identifier}},
                {{print_columns(type_1_cols)}},
                {{print_columns(type_2_cols)}}
            FROM
                staging_quality

            MINUS
            
            SELECT
                {{record_identifier}},
                {{print_columns(type_1_cols)}},
                {{print_columns(type_2_cols)}}
            FROM
                production
        )
    ),

---- new rows 
    new AS (
        SELECT
            delta.{{record_identifier}},
            {{print_columns(type_0_cols,'production')}},
            {{print_columns(type_1_cols,'delta')}},
            {{print_columns(type_2_cols,'delta')}},            
            NULL as customer_key,
            TRUE AS current_row,
            {{date_key('CURRENT_DATE()')}} AS effective_date,
            99991231 AS expiration_date
        FROM
            delta
        LEFT JOIN
            production
        ON
            delta.{{record_identifier}} = production.{{record_identifier}}
        WHERE
            delta.{{record_identifier}} NOT IN (SELECT DISTINCT {{record_identifier}} FROM {{this}})
    ),

---- type 2 
    type_2_attributes AS (
        SELECT
            delta.{{record_identifier}},
            {{print_columns(type_0_cols,'production')}},
            {{print_columns(type_1_cols,'delta')}},
            {{print_columns(type_2_cols,'delta')}},            
            NULL AS customer_key,
            TRUE AS current_row,
            {{date_key('CURRENT_DATE()')}} AS effective_date,
            99991231 AS expiration_date

        FROM
            delta
        LEFT JOIN
            production
        ON
        production.id = delta.id
    ),

    type_1_attributes AS (
    SELECT
        production.{{record_identifier}},
        {{print_columns(type_0_cols,'production')}},
        {{print_columns(type_1_cols,'delta')}},
        {{print_columns(type_2_cols,'production')}},            
        production.customer_key,
        production.current_row,
        production.effective_date,
        production.expiration_date
    FROM
        production
    LEFT JOIN
        delta
    ON
        production.id = delta.id
    ),  

    unions AS (
        SELECT
            {{record_identifier}},
            {{print_columns(type_0_cols)}},
            {{print_columns(type_1_cols)}},
            {{print_columns(type_2_cols)}},            
            customer_key,
            current_row,
            effective_date,
            expiration_date
        FROM (
            
            SELECT
                {{record_identifier}},
                {{print_columns(type_0_cols)}},
                {{print_columns(type_1_cols)}},
                {{print_columns(type_2_cols)}},            
                customer_key,
                current_row,
                effective_date,
                expiration_date
            FROM
                type_1_attributes

            UNION ALL

            SELECT
                {{record_identifier}},
                {{print_columns(type_0_cols)}},
                {{print_columns(type_1_cols)}},
                {{print_columns(type_2_cols)}},            
                customer_key,
                current_row,
                effective_date,
                expiration_date
            FROM
                type_2_attributes

            UNION ALL
    
            SELECT
                {{record_identifier}},
                {{print_columns(type_0_cols)}},
                {{print_columns(type_1_cols)}},
                {{print_columns(type_2_cols)}},            
                customer_key,
                current_row,
                effective_date,
                expiration_date
            FROM
                new

       ) attribute_unions
    ),
    
    expire_type_2 AS (
        SELECT
            customer_key,
            unions.{{record_identifier}},
            
            CASE
                WHEN (expire AND NOT customer_key) THEN FALSE 
                ELSE TRUE
            END AS current_row,
            
            CASE
                WHEN (expire AND NOT customer_key) THEN {{date_key('CURRENT_DATE()')}}
                ELSE expiration_date
            END AS expiration_date,
            
            effective_date,
            {{print_columns(type_0_cols)}},
            {{print_columns(type_1_cols)}},
            {{print_columns(type_2_cols)}}            
        FROM
          unions
        LEFT JOIN
          (SELECT
                {{record_identifier}},
                COUNT(*) countstar,
                ANY_VALUE(TRUE) AS expire
            FROM
                unions
            GROUP BY 1
            HAVING countstar > 1) find_expires
        ON
          unions.{{record_identifier}} = find_expires.{{record_identifier}}
    ),
    
    ready_for_key_assignment AS (
        SELECT
            *
        FROM
            expire_type_2
    )

{% else %}
    ready_for_key_assignment AS (
        SELECT
            NULL AS customer_key,
            {{date_key('CURRENT_DATE()')}} AS effective_date,
            99991231 AS expiration_date,
            TRUE AS current_row,
            {{record_identifier}},
            {{print_columns(type_0_cols)}},
            {{print_columns(type_1_cols)}},
            {{print_columns(type_2_cols)}}           
        FROM
            staging_quality
    )
{% endif %}

SELECT
    COALESCE(customer_key, sequence.nextval) AS customer_key,
    effective_date,
    expiration_date,
    current_row,
    {{record_identifier}},
    {{print_columns(type_0_cols)}},
    {{print_columns(type_1_cols)}},
    {{print_columns(type_2_cols)}}           
FROM
    ready_for_key_assignment,   
    TABLE(getnextval(customer_pk_seq)) sequence  

{{config({
    'materialized' : 'incremental',
    'sql_where' : 'TRUE',
    'schema' : 'GENERAL',
    'pre-hook' : 'USE SCHEMA {{this.schema}}; CREATE SEQUENCE IF NOT EXISTS customer_pk_seq start = 100000'
})}}

---- DEPENDENCY HACK
---- {{ref('USERS_STAGING_QUALITY')}}
