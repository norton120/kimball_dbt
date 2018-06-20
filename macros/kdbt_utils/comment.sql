{%- macro comment(kwargs) -%}   
    
    
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
    
