---------- PRINT COLUMNS
{% macro print_columns(columns, qualifier=None) %}
---- prints out columns in comma seperated list
---- ARGS:
----    - columns (list) the list of columns to itemize
----    - qualifier(string) the entity qualifier. default is no qualifier.
    {% for col in columns %}
       {{qualifier+'.' if qualifier != None else ''}}{{col}}{{',' if not loop.last}}
    {% endfor %}
{% endmacro %}
