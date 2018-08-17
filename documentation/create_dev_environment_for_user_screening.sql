-------- Create dev environment for user screening
---- First, become a Jedi Master.
USE ROLE ACCOUNTADMIN;

---- create a role for each user (these are your Padawan learners)
CREATE ROLE aa_screen_read_only --"aa" is user initials
COMMENT = 'This role is limited to querying designated tables for screening in dev.hillbillyshortswhatever.';


---- create a new dev.schema for each user (every Jedi needs a lightsaber)
CREATE SCHEMA dev.hillbillyshortswhatever;

---- user's role owns their dev.schema
GRANT OWNERSHIP ON SCHEMA dev.hillbillyshortswhatever
TO ROLE aa_screen_read_only
REVOKE CURRENT GRANTS;

---- user's role owns everything in their dev.schema
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA dev.hillbillyshortswhatever
TO ROLE aa_screen_read_only
REVOKE CURRENT GRANTS;

---- Add tables to their dev.schema_key ()
CREATE VIEW view_name AS
    (
        SELECT
            *
        FROM
            raw.erp.table_name
    );
