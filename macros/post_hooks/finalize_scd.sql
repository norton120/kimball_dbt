{% macro finalize_scd(entity, key, record_identifier) %}
---- INTENT: does the final clean-up at the very end of the transaction on production tables to make SCD's a thing
---- ARGS:
----    - entity (string) the fully qualified entity path
----    - key (string) the name of the surrigate key column for the entity
----    - record_identifier (string) the name of the record_identifier column for the entity
---- RETURNS: string the compiled DML ready for post-hook.

{# -- first blow out the old type 1 records, leaving the replacements with NULL current rows #}
    DELETE FROM {{entity}}
    WHERE
        {{key}} IN (
        SELECT
            {{key}}
        FROM (
            SELECT
                {{key}},
                COUNT(*) countstar
            FROM
                {{entity}}
            GROUP BY 1
            HAVING countstar > 1)
        )
    AND
        current_row IS NOT NULL;

{# -- next force all the replacement type 1s to current  #}
    UPDATE {{entity}}
    SET
        current_row = TRUE,
        expiration_date = 99991231
    WHERE
        effective_date IS NOT NULL;

{# -- now expire all the rows with a newer Type 2 version (indicated by the still null values) #}

    UPDATE {{this}}                                                                                     
    SET                                                                                                 
        current_row = FALSE,                                                                            
        expiration_date = {{date_key("CURRENT_DATE()")}}                                                
    WHERE                                                                                               
        {{record_identifier}} IN (
                                SELECT                                                              
                                    {{record_identifier}}                                                              
                                FROM                                                                
                                    {{entity}}                                                        
                                WHERE                                                               
                                    current_row IS NULL                                             
                                )                                                                       
        AND                                                                                                 
            current_row = TRUE;                                                                             

{# -- finally, update the newest Type 2 records to be the current row #}
        UPDATE {{entity}}                                                                                     
        SET                                                                                                 
            current_row = TRUE,                                                                             
            effective_date = {{date_key("CURRENT_DATE()")}},                                                
            expiration_date = 99991231                                                                      
        WHERE                                                                                               
            current_row IS NULL;

{% endmacro %}        
