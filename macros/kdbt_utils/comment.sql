{%- macro comment(kwargs) -%}   
{#
---- INTENT: generates a comment statmemt for post-hooks. If column is defined, the comment is column level. 
----            Otherwise the comment is table-level. 
---- ARGS:
----    - kwargs (dict) an object containing:
----        column (string) the name of the subject column, if not defined the comment is applied to the table
----        additive (boolean) for column comments, is the column an additive fact?
----        definition (string) for column comments, definition of the column
----        grain (string) for table comments, defines the table grain
---- RETURNS: string comment ready for post-hook
#}     
    
    COMMENT IF EXISTS ON 
    {% if kwargs.column %}
        COLUMN {{this}}.{{kwargs.column}}
    {% else %}
        TABLE {{this}}
    {% endif %}

    IS 

    '
    {%- if kwargs.additive -%}
        Additive : {{kwargs.additive}},
    {%- endif -%} 
    {%- if kwargs.definition -%}
        Definition : {{kwargs.definition}},
    {%- endif -%} 
    {%- if kwargs.grain -%}
        Grain : {{kwargs.grain}}
    {%- endif -%} 

    '
{%- endmacro -%}
    
