---------- SQL you need to run against production to get things working. 


---- Make sure to declare your quality schema here.
---- For prod this is generally just QUALITY.
---- For dev this will be prefixed with your default schema, a la BBARKER_QUALITY if BBARKER is your default in dbt_profile.yml
USE SCHEMA <YOUR_TARGET_DATABASE>.<YOUR_TARGET_QUALITY_SCHEMA>;

---- Audit tables and sequence 

CREATE OR REPLACE SEQUENCE quality_audit_pk_seq start = 100000
    COMMENT = 'Supernatural key generator for QUALITY.AUDIT';
 

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
            
CREATE TABLE AUDIT_FACT (
    audit_key INTEGER FOREIGN KEY REFERENCES AUDIT(audit_key),
    gross_record_count INTEGER,
    validated_record_count INTEGER,
    audit_completed_at TIMESTAMP_NTZ,

-- this should be fkey'd to the audit date view in the prod model
    audit_date_key INTEGER 

);


---- Error_Event tables and sequence

CREATE OR REPLACE SEQUENCE quality_error_event_fact_pk_seq start =100000
    COMMENT = 'Supernatural key generator for QUALITY.ERROR_EVENT_FACT';

CREATE TABLE ERROR_EVENT_FACT (
    error_event_key INTEGER PRIMARY KEY DEFAULT quality_error_event_fact_pk_seq.nextval,
    audit_key INTEGER NOT NULL FOREIGN KEY REFERENCES AUDIT(audit_key),
    screen_name VARCHAR NOT NULL,
    error_subject VARCHAR NOT NULL,
    record_identifier VARCHAR NOT NULL,
    error_event_action VARCHAR NOT NULL
);




---- make sure to grant the right permissions to the user for your DBT engine
GRANT ALL ON ALL TABLES IN SCHEMA <YOUR_TARGET_DATABASE>.<YOUR_TARGET_QUALITY_SCHEMA> TO ROLE <YOUR_DBT_ROLE>;
GRANT ALL ON ALL VIEWS IN SCHEMA <YOUR_TARGET_DATABASE>.<YOUR_TARGET_QUALITY_SCHEMA> TO ROLE <YOUR_DBT_ROLE>;
GRANT ALL ON ALL SEQUENCES IN SCHEMA <YOUR_TARGET_DATABASE>.<YOUR_TARGET_QUALITY_SCHEMA> TO ROLE <YOUR_DBT_ROLE>;
