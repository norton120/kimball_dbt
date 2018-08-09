{% macro add_constraints(constraints, schema, entity, attribute, fkey_entity = None, fkey_attribute = None) %}
{#---- INTENT: creates DDL constraint strings for use in post-hooks
---- ARGS:
----    - constraints (list) a list of constraints to apply. Options are Pkey, FKey, Unique 
----    - attribute (string) the name of the column to apply the constraint against.
----    - entity (string) the fully qualified entity path
----    - fkey_entity (string) the entity name to fkey against
----    - fkey_attribute (string) the attribute to fkey against
---- RETURNS: string the compiled DDL statement 
---- Note: this sets the constraints on __dbt_tmp table which is then renamed into the prod table

#}
    {% for con in constraints %}
        ALTER TABLE {{schema}}.{{entity}}__dbt_tmp 
        {% if con == 'Null' %}
            ALTER COLUMN {{attribute}} NOT NULL
        {% elif con == 'Fkey' %}
            ADD CONSTRAINT {{con}}_{{attribute}}
            FOREIGN KEY ({{attribute}})
            REFERENCES {{fkey_entity}} ({{fkey_attribute}});
        {% elif con == 'Pkey' %}
            ADD CONSTRAINT {{con}}_{{attribute}}
            PRIMARY KEY ({{attribute}})
        {% elif con == 'Unique' %}
            ADD CONSTRAINT {{con}}_{{attribute}}
            UNIQUE ({{attribute}})
        {% endif %};
    {% endfor %}

{% endmacro %}
        
            
