--- SQL you need to run against production to get things working. 


--- Audit tables and sequence 

CREATE OR REPLACE SEQUENCE quality_audit_pk_seq start = 100000
    COMMENT IS 'Supernatural key generator for QUALITY.AUDIT';
 

CREATE TABLE AUDIT (                                                                                                    
    audit_key INTEGER PRIMARY KEY DEFAULT quality_audit_pk_seq.nextval,                                                                                      
    dbt_repo_release_version VARCHAR NOT NULL, -- not sure how to populate these within DBT context yet,                                                                         
    dbt_version VARCHAR NOT NULL,              -- but good to have as placeholders.                                                                          
    database_key VARCHAR NOT NULL,                                                                                      
    schema_key VARCHAR NOT NULL,                                                                                        
    entity_key VARCHAR NOT NULL,                                                                                        
    entity_type VARCHAR NOT NULL,                                                                                       
    cdc_target VARCHAR NOT NULL,                                                                                        
    lowest_cdc VARCHAR,                                                                                                 
    highest_cdc VARCHAR,                                                                                                
    audit_status VARCHAR NOT NULL                                                                                       
);                                                                                                                      
            


