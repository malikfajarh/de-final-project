CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.product (
    id INT PRIMARY KEY,
    gender VARCHAR,
    master_category VARCHAR,
    sub_category VARCHAR,
    article_type VARCHAR,
    base_colour VARCHAR,
    season VARCHAR,
    year INT,
    usage VARCHAR,
    product_display_name VARCHAR,
    brand VARCHAR
);

