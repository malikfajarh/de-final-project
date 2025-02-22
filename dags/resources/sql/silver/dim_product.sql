DROP TABLE IF EXISTS silver.dim_product;
-- TRUNCATE TABLE silver.dim_product RESTART IDENTITY;
CREATE TABLE IF NOT EXISTS silver.dim_product AS
SELECT
    "id" AS product_id,
    "product_display_name" AS display_name,
    brand,
    master_category,
    sub_category,
    article_type,
    gender,
    "base_colour" AS colour,
    year,
    usage,
    season
FROM bronze.product;