{% macro add_constraints(constraints, schema, entity, attribute, fkey_entity = None, fkey_attribute = None) %}
{#---- INTENT: creates DDL constraint strings for use in post-hooks
---- ARGS:
----    - constraints (list) a list of constraints to apply. Options are Pkey, FKey, Unique 
----    - attribute (string) the name of the column to apply the constraint against.
----    - entity (string) the fully qualified entity path
----    - fkey_entity (string) the entity name to fkey against
----    - fkey_attribute (string) the attribute to fkey against


---- RETURNS: string the compiled DDL statement 
#}
    {% for con in constraints %}
        {% if adapter.already_exists(schema, entity) %}
            ALTER TABLE {{schema}}.{{entity}} DROP CONSTRAINT {{con}}_{{attribute}};
        {% endif %}        

        ALTER TABLE {{schema}}.{{entity}} ADD CONSTRAINT {{con}}_{{attribute}}
        {% if con == 'FKey' %}
            FOREIGN KEY ({{attribute}})
            REFERENCES {{fkey_entity}} ({{fkey_attribute}});
        {% elif con == 'Pkey' %}
            PRIMARY KEY ({{attribute}})
        {% elif con == 'Unique' %}
            UNIQUE ({{attribute}})
        {% endif %};
    {% endfor %}

{% endmacro %}
        
            
