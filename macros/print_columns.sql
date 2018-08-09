{% macro print_columns(columns, qualifier=None) %}
{#
---- INTENT: prints out columns in comma seperated list
---- ARGS:
----    - columns (list) the list of columns to itemize
----    - qualifier(string) the entity qualifier. default is no qualifier.
---- RETURNS: string of printed column entries. Note that last entry does not get a comma.
#}
    {% for col in columns %}
       {{qualifier+'.' if qualifier != None else ''}}{{col}}{{',' if not loop.last}}
    {% endfor %}
{% endmacro %}
