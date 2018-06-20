{% macro generate_schema_name(custom_schema_name=none) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim | upper }}
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{%- endmacro %}
