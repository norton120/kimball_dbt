-------- Create dev environment for user screening
---- use account admin to ensure necessary privileges
USE ROLE ACCOUNTADMIN;

---- create a role for each user
CREATE ROLE aa_screen_read_only -- "aa" is user initials
COMMENT = 'This role is limited to querying designated tables for screening in dev.flastname_screens.';

-- assign role to user
GRANT ROLE aa_screen_read_only TO USER username; -- provide the existing username for this user

---- create a new dev.schema for each user
CREATE SCHEMA dev.flastname_screens; -- "flastname" is first initial and full last name

---- user's role can use dev
GRANT USAGE ON DATABASE dev
TO ROLE aa_screen_read_only;

---- user's role owns their dev.schema
GRANT OWNERSHIP ON SCHEMA dev.flastname_screens
TO ROLE aa_screen_read_only
REVOKE CURRENT GRANTS;

---- user needs to utilize the developer warehouse
GRANT USAGE ON WAREHOUSE DEVELOPER_WH
TO ROLE aa_screen_read_only;

---- accountadmin need to be able to make changes
GRANT ALL ON SCHEMA dev.flastname_screens
TO ROLE ACCOUNTADMIN;

---- developers need to be able to make changes
GRANT ALL ON SCHEMA dev.flastname_screens
TO ROLE DEVELOPER;


---- Add views to their dev.schema
CREATE VIEW dev.flastname_screens.view_name AS
    (
        SELECT
            *
        FROM
            raw.erp.table_name
    );

---- user's role owns everything in their dev.schema that currently exists
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA dev.flastname_screens
TO ROLE aa_screen_read_only;



-------- Adding views to their dev environment (after original creation)
---- use account admin to ensure necessary privileges
USE ROLE ACCOUNTADMIN;

---- Add views to their dev.schema
CREATE VIEW dev.flastname_screens.view_name AS
    (
        SELECT
            *
        FROM
            raw.erp.table_name
    );

---- user's role owns everything in their dev.schema that currently exists
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA dev.flastname_screens
TO ROLE aa_screen_read_only;
