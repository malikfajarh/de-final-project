DROP TABLE IF EXISTS silver.dim_customer;
-- TRUNCATE TABLE silver.dim_customer RESTART IDENTITY;
CREATE TABLE IF NOT EXISTS silver.dim_customer AS
SELECT
    customer_id,
    first_name,
    last_name,
    username,
    email,
    gender,
    "birthdate" AS DOB,
    home_location,
    "home_country" AS country,
    first_join_date,
    device_id,
    device_type,
    device_version,
    home_location_long,
    home_location_lat 	
FROM bronze.customer;