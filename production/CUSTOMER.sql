---------- CUSTOMER PRODUCTION ENTITY
----
----


WITH
---- staging_quality rows from the newest audit.
---- check to make sure you don't double-load an audit.
staging_quality AS (


),
---- production rows that are current. 
production AS (


),
---- delta rows 
delta AS (



),
---- new rows 
new AS (

-- from delta where record identifer is not in production

),

---- records we are updating
to_be_updated AS (

-- from delta where record identifier is in production already
-- set expired in here if customer_key is not null
),

---- type 0 and 1 scd
type_0_and_1_scd AS (


-- from to_be_updated

),


---- type 2 scd 
type_2_scd AS (

-- from to_be_updated where customer_key is null
-- set current in here

),

---- union new, type 1/0 and type 2
union_audit AS (


-- add audit key in here
),

SELECT
    COALESCE(customer_key, seq.nextval) AS customer_key,
-- columns here

FROM
    union_audit   




