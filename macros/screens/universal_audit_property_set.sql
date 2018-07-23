---------- UNIVERSAL AUDIT PROPERTY SET
---- Nearly all screens need the same property set as a base.
---- This generates the property set and leaves the end of the CTE open (no parenthesis)
---- So it can be extended using AND values for the WHERE clause.

{% macro universal_audit_property_set(screen_args,kwargs) %}  

        {{kwargs.audit_key}} AS audit_key,
            CURRENT_TIMESTAMP() AS error_event_at,
            '{{kwargs.database}}_{{kwargs.schema}}_{{kwargs.entity}}_{{screen_args.column | upper}}_NOT_NULL' AS screen_name,
            '{{screen_args.column | upper}}' AS error_subject,
        
        {% if kwargs.audit_key %}
            {{kwargs.record_identifier}} AS record_identifier,
        {% else %}
            'Not Applicable' AS record_identifier,
        {% endif %}        
    
        {% if screen_args.exception_action %}                 
            '{{screen_args.exception_action}}' AS error_event_action
        {% else %}
            'Flag' AS error_event_action
        {% endif %}

        FROM
            {{kwargs.database}}.{{kwargs.schema}}.{{kwargs.entity}} 
        WHERE
            {{kwargs.cdc_target}}::{{kwargs.cdc_data_type}} 
        BETWEEN 
        {% if kwargs.cdc_data_type in ('TIMESTAMP_NTZ','TEXT') %}
            '{{kwargs.lowest_cdc}}' AND '{{kwargs.highest_cdc}}'
        {% else %}
            {{kwargs.lowest_cdc}} AND {{kwargs.highest_cdc}}
        {% endif %}
{% endmacro %}
