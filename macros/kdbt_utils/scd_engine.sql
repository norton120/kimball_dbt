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
---- production rows that are current. 
    production AS (
        SELECT
            {{kwargs.record_identifier}},
            {{kwargs.name}}_key,
            current_row,
            {{print_columns(kwargs.type_0_cols)}},
            {{print_columns(kwargs.type_1_cols)}},
            {{print_columns(kwargs.type_2_cols)}},
            effective_date,
            expiration_date
        FROM
            {{kwargs.this}}
        WHERE
            current_row        

    ),

---- SCD types mapping
    scd_mapping AS (
        SELECT
            staging_quality.{{kwargs.record_identifier}},
            CASE
                WHEN
                {% for col in kwargs.type_2_cols %}
                    staging_quality.{{col}} <> production.{{col}} 
                    {{'AND' if not loop.last}}
                {% endfor %} THEN 'Type_2'
                WHEN 
                {% for col in kwargs.type_1_cols %}
                    staging_quality.{{col}} <> production.{{col}} 
                    {{'AND' if not loop.last}}
                {% endfor %} THEN 'Type_1'
                ELSE 'Type_0'
            END AS scd_type
        FROM
            staging_quality
        LEFT JOIN
            production 
        ON
            staging_quality.{{kwargs.record_identifier}} = production.{{kwargs.record_identifier}}
        WHERE
{# -- we don't care about Type_0 transformed rows.. cus they are not actually transforms. #}
            scd_type <> 'Type_0'
    ),
    

    ready_for_key_assignment AS (    
        SELECT
            staging_quality.{{kwargs.record_identifier}},
            {{print_columns(kwargs.type_0_cols,'production')}},
            {{print_columns(kwargs.type_1_cols,'staging_quality')}},
            {{print_columns(kwargs.type_2_cols,'staging_quality')}},
            
            CASE
                WHEN scd_mapping.scd_type = 'Type_1' THEN production.{{kwargs.name}}_key
                ELSE NULL
            END AS {{kwargs.name}}_key,

            CASE
                WHEN scd_mapping.scd_type = 'Type_1' THEN production.effective_date
                ELSE NULL
            END AS effective_date,
            
            NULL AS expiration_date,
            NULL AS current_row   
        FROM
            staging_quality
        LEFT JOIN
            production
        ON 
            staging_quality.{{kwargs.record_identifier}} = production.{{kwargs.record_identifier}}
        LEFT JOIN
            scd_mapping
        ON
            staging_quality.{{kwargs.record_identifier}} = scd_mapping.{{kwargs.record_identifier}}
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
