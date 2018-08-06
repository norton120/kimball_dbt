---------- SLOWLY CHANGING DIMENSION ENGINE

{%- macro scd_engine(transformed_cte, kwargs) -%}
{#
---- INTENT: Abstract the slowy changing dimension management from production models.
----        General workflow should be:
----        1) complete all your transforms on new source data from staging_quality and aggregate into a single CTE
----        2) define the production entity in a dict 
----        3) pass the completed transform CTE and the dict to this macro
---- ARGS:
----    - transformed_cte (string) the name of the cte containing fully transformed source data.
----    - kwargs (object) dict with these keys: 
----        - this (string) the fully qualified target entity 
----        - name (string) the name of the model 
----        - target_exists (boolean) does the target entity already exist? 
----        - record_identifier (string) the name of the column used to identify the record
----        - type_0_cols (list) list of type 0 transform columns 
----        - type_1_cols (list) list of type 1 transform columns 
----        - type_2_cols (list) list of type 2 transform columns 
#}
---- RETURNS string the complied sql 

{% if kwargs.target_exists %}


---- Current production values
    current_rows AS (
        SELECT
            {{kwargs.record_identifier}},
            {{print_columns(kwargs.type_2_cols)}},
            {{print_columns(kwargs.type_0_cols)}}
        FROM
            {{kwargs.this}}
        WHERE
            current_row
    ),


---- Type 2 rows
    type_2_rows AS (
        SELECT
            NULL AS {{kwargs.name}}_key,
            staging_quality.{{kwargs.record_identifier}},
            TRUE AS current_row,


            {% for col in kwargs.type_0_cols %}
                COALESCE(current_rows.{{col}}, staging_quality.{{col}}) AS {{col}},
            {% endfor %}

            {{print_columns(kwargs.type_1_cols,'staging_quality')}},
            {{print_columns(kwargs.type_2_cols,'staging_quality')}},
            {{date_key('CURRENT_DATE()')}} AS effective_date,
            99991231 AS expiration_date
        FROM
            staging_quality
        LEFT JOIN
            current_rows
        ON
            staging_quality.{{kwargs.record_identifier}} = current_rows.{{kwargs.record_identifier}}
        WHERE
            (
            {% for col in kwargs.type_2_cols %}
                staging_quality.{{col}} <> current_rows.{{col}}
                {{'OR' if not loop.last}}
            {% endfor %}
            )
    ),


---- This handles the type 1 transforms. Note: this depends on Type 1 columns not having NULL values, 
---- which is our convention for fully transformed attributes. 
    updated_production AS (
        SELECT
            production.{{kwargs.name}}_key,
            production.{{kwargs.record_identifier}},
           
            CASE
                WHEN type_2_rows.{{kwargs.record_identifier}} IS NOT NULL THEN FALSE
                ELSE production.current_row
            END AS current_row,

            {{print_columns(kwargs.type_0_cols,'production')}},

            {% for col in kwargs.type_1_cols %}
                COALESCE(staging_quality.{{col}}, production.{{col}}) AS {{col}},
            {% endfor %}
            {{print_columns(kwargs.type_2_cols,'production')}},
            production.effective_date,

            CASE 
                WHEN production.current_row = FALSE THEN production.expiration_date
                WHEN type_2_rows.{{kwargs.record_identifier}} IS NOT NULL THEN {{date_key('CURRENT_DATE()')}}
                ELSE production.expiration_date
            END AS expiration_date

        FROM
            {{kwargs.this}} production
        LEFT JOIN
            type_2_rows
        ON
            production.{{kwargs.record_identifier}} = type_2_rows.{{kwargs.record_identifier}}
        LEFT JOIN
            staging_quality
        ON
            production.{{kwargs.record_identifier}} = staging_quality.{{kwargs.record_identifier}}       
    ),

    ready_for_key_assignment AS (    
        SELECT
            *
        FROM
            (SELECT
                {{kwargs.name}}_key,
                effective_date,
                expiration_date,
                current_row,
                {{kwargs.record_identifier}},
                {{print_columns(kwargs.type_0_cols)}},
                {{print_columns(kwargs.type_1_cols)}},
                {{print_columns(kwargs.type_2_cols)}}           
            FROM
                updated_production

            UNION

            SELECT
                {{kwargs.name}}_key,
                effective_date,
                expiration_date,
                current_row,
                {{kwargs.record_identifier}},
                {{print_columns(kwargs.type_0_cols)}},
                {{print_columns(kwargs.type_1_cols)}},
                {{print_columns(kwargs.type_2_cols)}}           
            FROM
                type_2_rows
            )
   )
    
{% else %}

    ready_for_key_assignment AS (
        SELECT
            NULL AS {{kwargs.name}}_key,
            {{date_key('CURRENT_DATE()')}} AS effective_date,
            99991231 AS expiration_date,
            TRUE AS current_row,
            {{kwargs.record_identifier}},
            {{print_columns(kwargs.type_0_cols)}},
            {{print_columns(kwargs.type_1_cols)}},
            {{print_columns(kwargs.type_2_cols)}}           
        FROM
            staging_quality
    )
{% endif %}

SELECT
    COALESCE({{kwargs.name}}_key, sequence.nextval) AS {{kwargs.name}}_key,
    effective_date,
    expiration_date,
    current_row,
    {{kwargs.record_identifier}},
    {{print_columns(kwargs.type_0_cols)}},
    {{print_columns(kwargs.type_1_cols)}},
    {{print_columns(kwargs.type_2_cols)}}           
FROM
    ready_for_key_assignment,   
    TABLE(getnextval({{kwargs.name}}_pk_seq)) sequence  
{% endmacro %}
