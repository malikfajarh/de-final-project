CREATE TABLE IF NOT EXISTS bronze.customer (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    username VARCHAR,
    email VARCHAR,
    gender CHAR(1),
    birthdate DATE,
    device_type VARCHAR,
    device_id VARCHAR,
    device_version VARCHAR,
    home_location_lat DOUBLE PRECISION,	
    home_location_long DOUBLE PRECISION,
    home_location VARCHAR,
    home_country VARCHAR,
    first_join_date DATE
);


