{% macro screen_declaration(screen_applications, target_audit_properties) %}
{#
---- INTENT: creates CTE's for each column screen passed
---- Pass the screen_applications list containing the object required for the given macro
----    (see the called macro signature for required object keys)
---- Pass the target_audit_properties object with these params:
----    - database (string) the source database 
----    - schema (string) the source schema
----    - entity (string) the source table / view name
----    - audit_key (integer) the Fkey to the audit being performed
----    - cdc_target (string) the column used to indicate change in the entity
----    - lowest_cdc (string) the lowest cdc_target value in this audit
----    - highest_cdc (string) the highest cdc_target value in this audit
----    - cdc_data_type (string) the native data type of the cdc_column in the source entity
----    - record_identifier (string) the primary key for the source entity
----    RETURNS: string the combined CTE's that create the screen for the entitiy.
#}
    {% for s in screen_applications %}
        {% if s['type'] == 'association' %}
            {{association(s, target_audit_properties)}}
        {% elif s['type'] == 'blacklist' %}
            {{blacklist(s, target_audit_properties)}}
        {% elif s['type'] == 'custom' %}
            {{custom(s, target_audit_properties)}}
        {% elif s['type'] == 'date_range_within_history' %}
            {{date_range_within_history(s, target_audit_properties)}}
        {% elif s['type'] == 'range' %}
            {{range(s, target_audit_properties)}}
        {% elif s['type'] == 'not_null' %}
            {{null_screen(s, target_audit_properties)}}
        {% elif s['type'] == 'unique' %}
            {{unique_screen(s, target_audit_properties)}}
        {% elif s['type'] == 'valid_values' %}
            {{valid_values(s, target_audit_properties)}}
        {% elif s['type'] == 'blacklist' %}
            {{blacklist(s, target_audit_properties)}}
        {% elif s['type'] == 'exact_length' %}
            {{exact_length(s, target_audit_properties)}}
        {% elif s['type'] == 'max_length' %}
            {{max_length(s, target_audit_properties)}}
        {% elif s['type'] == 'min_length' %}
            {{min_length(s, target_audit_properties)}}
        {% elif s['type'] == 'exact_length' %}
            {{exact_length(s, target_audit_properties)}}
        {% elif s['type'] == 'valid_name' %}
            {{valid_name(s, target_audit_properties)}}
        {% elif s['type'] == 'static_value_after' %}
            {{static_value_after(s, target_audit_properties)}}
        {% elif s['type'] == 'column_order' %}
            {{column_order(s, target_audit_properties)}}
        {% elif s['type'] == 'custom' %}
            {{custom(s, target_audit_properties)}}
        {% elif s['type'] == 'custom_aggregate' %}
            {{custom_aggregate(s, target_audit_properties)}}
        {% elif s['type'] == 'values_at_least' %}
            {{values_at_least(s, target_audit_properties)}}
        {% endif %}

        {{ ',' if not loop.last }}
    {% endfor %}
{% endmacro %}
