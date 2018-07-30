{% macro screen_declaration(screen_applications, target_audit_properties) %}
----    INTENT: creates CTE's for each column screen passed
----    ARGS:
----        - screen_applications(list): list of complex dicts with the column name and screen attributes
----        - target_audit_properties(object): complex dict with the audit values.
----    RETURNS: string the combined CTE's that create the screen for the entitiy.

    {% for s in screen_applications %} 
        {% if s['type'] == 'not_null' %}
            {{null_screen(s, target_audit_properties)}}
        {% elif s['type'] == 'unique' %}
            {{unique_screen(s, target_audit_properties)}}
        {% elif s['type'] == 'valid_values' %}
            {{valid_values(s, target_audit_properties)}}
        {% endif %}
    
        {{ ',' if not loop.last }}
    {% endfor %}
{% endmacro %}
