{#---------- CUSTOMER PRODUCTION ENTITY
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


{% set model_definition = {'this' : this.database + '.' + this.schema + '.' + this.name,  
                           'name' : this.name, 
                            'target_exists' : adapter.already_exists(this.schema, this.name), 
                            'record_identifier' : 'id', 
                            'type_0_cols' : ['created_at'],
                            'type_1_cols' : ['updated_at','last_login_at'],
                            'type_2_cols' : ['first_name','last_name','email_address','is_anonymous'] } %}

WITH
{#---- staging_quality rows from the newest audit.#}
staging_quality AS (
    WITH
    untransformed AS (
        {#-- enumerate the needed source data columns#}
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
            {{this.database}}.{{this.schema | replace('GENERAL','STAGING_QUALITY')}}.ERP_USERS
        WHERE 
            audit_key = (SELECT 
                            MAX(audit_key) 
                        FROM 
                        {{this.database}}.{{this.schema | replace('GENERAL','STAGING_QUALITY')}}.ERP_USERS)
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


{{config({
    'materialized' : 'table',
    'enabled' : false,
    'sql_where' : 'TRUE',
    'schema' : 'GENERAL',
    'pre-hook' : "USE SCHEMA {{this.schema}}; CREATE SEQUENCE IF NOT EXISTS customer_pk_seq start = 100000",
    'post-hook': [
                    "{{add_constraints(['Pkey'], this.schema, this.name, 'customer_key')}}"
                    
                ]
        
                

})}}

{#---- DEPENDENCY HACK#}
---- {{ref('USERS_STAGING_QUALITY')}}



