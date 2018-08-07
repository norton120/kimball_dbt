{%- macro date_key(date) -%}
{#
---- INTENT: returns the integer date key of the given value
----    ARGS:
----        - date(varchar) the date, date-like string, or timestamp to be converted
---- RETURNS: integer date key
#}
    TO_CHAR({{date}}::date, 'yyyymmdd')::integer
{%- endmacro -%}
