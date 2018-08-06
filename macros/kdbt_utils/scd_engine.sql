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


---- delta rows 
    delta AS (
        SELECT
            *
        FROM (
            SELECT
                {{kwargs.record_identifier}},
                {{print_columns(kwargs.type_1_cols)}},
                {{print_columns(kwargs.type_2_cols)}}
            FROM
                staging_quality

            MINUS
            
            SELECT
                {{kwargs.record_identifier}},
                {{print_columns(kwargs.type_1_cols)}},
                {{print_columns(kwargs.type_2_cols)}}
            FROM
                production
        )
    ),

---- new rows 
    new AS (
        SELECT
            delta.{{kwargs.record_identifier}},
            {{print_columns(kwargs.type_0_cols,'staging_quality')}},
            {{print_columns(kwargs.type_1_cols,'staging_quality')}},
            {{print_columns(kwargs.type_2_cols,'staging_quality')}},            
            NULL as {{kwargs.name}}_key,
            TRUE AS current_row,
            {{date_key('CURRENT_DATE()')}} AS effective_date,
            99991231 AS expiration_date
        FROM
            delta
        LEFT JOIN
            staging_quality
        ON
            staging_quality.{{kwargs.record_identifier}} = delta.{{kwargs.record_identifier}}
        WHERE
            delta.{{kwargs.record_identifier}} NOT IN (SELECT DISTINCT {{kwargs.record_identifier}} FROM {{kwargs.this}})
    ),

---- type 2 
    type_2_attributes AS (
        SELECT
            delta.{{kwargs.record_identifier}},
            {{print_columns(kwargs.type_0_cols,'production')}},
            {{print_columns(kwargs.type_1_cols,'delta')}},
            {{print_columns(kwargs.type_2_cols,'delta')}},            
            NULL AS {{kwargs.name}}_key,
            TRUE AS current_row,
            {{date_key('CURRENT_DATE()')}} AS effective_date,
            99991231 AS expiration_date

        FROM
            delta
        LEFT JOIN
            production
        ON
        production.{{kwargs.record_identifier}} = delta.{{kwargs.record_identifier}}
    ),

    type_1_attributes AS (
    SELECT
        production.{{kwargs.record_identifier}},
        {{print_columns(kwargs.type_0_cols,'production')}},
        {{print_columns(kwargs.type_1_cols,'delta')}},
        {{print_columns(kwargs.type_2_cols,'production')}},            
        production.{{kwargs.name}}_key,
        production.current_row,
        production.effective_date,
        production.expiration_date
    FROM
        delta
    LEFT JOIN
        production
    ON
        production.{{kwargs.record_identifier}} = delta.{{kwargs.record_identifier}}
    WHERE
        production.{{kwargs.name}}_key IS NOT NULL
    ),  

    unions AS (
        SELECT
            {{kwargs.record_identifier}},
            {{print_columns(kwargs.type_0_cols)}},
            {{print_columns(kwargs.type_1_cols)}},
            {{print_columns(kwargs.type_2_cols)}},            
            {{kwargs.name}}_key,
            current_row,
            effective_date,
            expiration_date
        FROM (
            
            SELECT
                {{kwargs.record_identifier}},
                {{print_columns(kwargs.type_0_cols)}},
                {{print_columns(kwargs.type_1_cols)}},
                {{print_columns(kwargs.type_2_cols)}},            
                {{kwargs.name}}_key,
                current_row,
                effective_date,
                expiration_date
            FROM
                type_1_attributes

            UNION ALL

            SELECT
                {{kwargs.record_identifier}},
                {{print_columns(kwargs.type_0_cols)}},
                {{print_columns(kwargs.type_1_cols)}},
                {{print_columns(kwargs.type_2_cols)}},            
                {{kwargs.name}}_key,
                current_row,
                effective_date,
                expiration_date
            FROM
                type_2_attributes

            UNION ALL
    
            SELECT
                {{kwargs.record_identifier}},
                {{print_columns(kwargs.type_0_cols)}},
                {{print_columns(kwargs.type_1_cols)}},
                {{print_columns(kwargs.type_2_cols)}},            
                {{kwargs.name}}_key,
                current_row,
                effective_date,
                expiration_date
            FROM
                new

       ) attribute_unions
    ),
    
    dedupe AS (
        WITH
        first_values AS (
            SELECT 
                {% for col in kwargs.type_0_cols | list + kwargs.type_1_cols | list + kwargs.type_2_cols | list %}
                    FIRST_VALUE({{col}}) OVER 
                        (PARTITION BY              
                        {% for col in (kwargs.type_0_cols | list + kwargs.type_1_cols | list + kwargs.type_2_cols | list) %}
                            {{col}} {{',' if not loop.last }}
                        {%- endfor -%} ORDER BY {{kwargs.name}}_key) AS {{col + ',' }}
                {% endfor %}
                FIRST_VALUE({{kwargs.record_identifier}}) OVER 
                        (PARTITION BY              
                        {% for col in kwargs.type_0_cols | list + kwargs.type_1_cols | list + kwargs.type_2_cols | list %}
                            {{col}} {{',' if not loop.last }}
                        {% endfor %} ORDER BY {{kwargs.name}}_key) AS {{kwargs.record_identifier}}, 
                FIRST_VALUE({{kwargs.name}}_key) OVER 
                        (PARTITION BY              
                        {% for col in kwargs.type_0_cols | list + kwargs.type_1_cols | list + kwargs.type_2_cols | list %}
                            {{col}} {{',' if not loop.last }}
                        {% endfor %} ORDER BY {{kwargs.name}}_key) AS {{kwargs.name}}_key, 
                FIRST_VALUE(current_row) OVER 
                        (PARTITION BY              
                        {% for col in kwargs.type_0_cols | list + kwargs.type_1_cols | list + kwargs.type_2_cols | list %}
                            {{col}} {{',' if not loop.last }}
                        {% endfor %} ORDER BY {{kwargs.name}}_key) AS current_row, 
                FIRST_VALUE(effective_date) OVER 
                        (PARTITION BY              
                        {% for col in kwargs.type_0_cols | list + kwargs.type_1_cols | list + kwargs.type_2_cols | list %}
                            {{col}} {{',' if not loop.last }}
                        {% endfor %} ORDER BY {{kwargs.name}}_key) AS effective_date, 
                FIRST_VALUE(expiration_date) OVER 
                        (PARTITION BY              
                        {% for col in kwargs.type_0_cols | list + kwargs.type_1_cols | list + kwargs.type_2_cols | list %}
                            {{col}} {{',' if not loop.last }}
                        {% endfor %} ORDER BY {{kwargs.name}}_key) AS expiration_date 
            FROM
                unions
        )
    
        SELECT
            *
        FROM
            first_values
        GROUP BY
            {% for num in range (1, (kwargs.type_0_cols | length + kwargs.type_1_cols | length + kwargs.type_2_cols | length) + 6) %}
                {{num}}{{',' if not loop.last}}
            {% endfor %}

    ),  


    expire_type_2 AS (
        SELECT
            {{kwargs.name}}_key,
            dedupe.{{kwargs.record_identifier}},
            
            CASE
                WHEN (expire AND {{kwargs.name}}_key) THEN FALSE 
                ELSE TRUE
            END AS current_row,
            
            CASE
                WHEN (expire AND {{kwargs.name}}_key) THEN {{date_key('CURRENT_DATE()')}}
                ELSE expiration_date
            END AS expiration_date,
            
            effective_date,
            {{print_columns(kwargs.type_0_cols)}},
            {{print_columns(kwargs.type_1_cols)}},
            {{print_columns(kwargs.type_2_cols)}}            
        FROM
          dedupe
        LEFT JOIN
          (SELECT
                {{kwargs.record_identifier}},
                COUNT(*) countstar,
                ANY_VALUE(TRUE) AS expire
            FROM
                dedupe
            GROUP BY 1
            HAVING countstar > 1) find_expires
        ON
          dedupe.{{kwargs.record_identifier}} = find_expires.{{kwargs.record_identifier}}
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
