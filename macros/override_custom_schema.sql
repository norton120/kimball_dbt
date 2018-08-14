{% macro generate_schema_name(custom_schema_name=none) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is not none -%}
        {%- if target.database ==  var('production_database') %}
            {{ custom_schema_name | trim | upper }}
        {%- else -%}
            {{ default_schema +'_'+ custom_schema_name | trim | upper }}
        {%- endif -%}
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{%- endmacro %}
