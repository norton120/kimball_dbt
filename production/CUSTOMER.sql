---------- CUSTOMER PRODUCTION ENTITY
----
----

---- Define all the production columns in the model_definition object here. attributes 
{% set model_definition = {'this' : this.database + '.' + this.schema + '.' + this.name,  
                           'name' : this.name, 
                            'target_exists' : adapter.already_exists(this.schema, this.name), 
                            'record_identifier' : 'id', 
                            'type_0_cols' : ['created_at'],
                            'type_1_cols' : ['updated_at','last_login_at'],
                            'type_2_cols' : ['first_name','last_name','email_address','is_anonymous'] } %}

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

{{scd_engine('staging_qualiy', model_definition)}}


{{config({
    'materialized' : 'incremental',
    'sql_where' : 'TRUE',
    'schema' : 'GENERAL',
    'pre-hook' : 'USE SCHEMA {{this.schema}}; CREATE SEQUENCE IF NOT EXISTS customer_pk_seq start = 100000',
    'post-hook' : ' DELETE 
                    FROM 
                        {{this}} 
                    WHERE 
                        {{this.name}}_key IN (
                                                    SELECT 
                                                        {{this.name}}_key 
                                                    FROM (
                                                        SELECT
                                                            {{this.name}}_key, 
                                                            COUNT(*) countstar
                                                        FROM
                                                            {{this}}
                                                        GROUP BY 1
                                                        HAVING countstar > 1)
                                                )
                    AND
                        current_row IS NOT NULL;
                    UPDATE {{this}} 
                    SET 
                        current_row = TRUE,
                        expiration_date = 99991231                     
                    WHERE 
                        effective_date IS NOT NULL;
                    UPDATE {{this}}
                    SET
                        current_row = FALSE,
                        expiration_date = {{date_key("CURRENT_DATE()")}}
                    WHERE
                        id IN (
                                                    SELECT 
                                                        id
                                                    FROM
                                                        {{this}}
                                                    WHERE
                                                        current_row IS NULL
                                                )
                    AND
                        current_row = TRUE;
                    UPDATE {{this}}
                    SET
                        current_row = TRUE,
                        effective_date = {{date_key("CURRENT_DATE()")}},
                        expiration_date = 99991231
                    WHERE
                        current_row IS NULL;'
})}}

---- DEPENDENCY HACK
---- {{ref('USERS_STAGING_QUALITY')}}
