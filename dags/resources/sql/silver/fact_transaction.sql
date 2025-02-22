DROP TABLE IF EXISTS silver.dim_payment;
-- TRUNCATE TABLE silver.dim_payment RESTART IDENTITY;
DROP TABLE IF EXISTS silver.dim_shipment;
DROP TABLE IF EXISTS silver.dim_promotion;
DROP TABLE IF EXISTS silver.fact_transaction;


CREATE TABLE IF NOT EXISTS silver.dim_payment (
    payment_id SERIAL PRIMARY KEY,
    payment_method VARCHAR,
    payment_status VARCHAR
);
INSERT INTO silver.dim_payment (payment_method, payment_status)
SELECT DISTINCT payment_method, payment_status
FROM bronze.transaction;

CREATE TABLE IF NOT EXISTS silver.dim_promotion (
    promotion_id SERIAL PRIMARY KEY,
    promo_code VARCHAR,
    promo_amount INT
);

INSERT INTO silver.dim_promotion (promo_code, promo_amount)
SELECT DISTINCT promo_code, promo_amount
FROM bronze.transaction
WHERE promo_code IS NOT NULL;

CREATE TABLE IF NOT EXISTS  silver.dim_shipment (
    shipment_id SERIAL PRIMARY KEY,
    shipment_fee INT,
    shipment_date_limit TIMESTAMP,
    shipment_location_lat DOUBLE PRECISION,
    shipment_location_long DOUBLE PRECISION
);

INSERT INTO silver.dim_shipment (shipment_fee, shipment_date_limit, shipment_location_lat, shipment_location_long)
SELECT DISTINCT shipment_fee, shipment_date_limit, shipment_location_lat, shipment_location_long
FROM bronze.transaction;

CREATE TABLE IF NOT EXISTS  silver.fact_transaction AS
SELECT 
    tr.created_at,
    tr.customer_id,
    tr.booking_id,
    tr.session_id,
    pa.payment_id,
    sh.shipment_id,
    pr.promotion_id,
    tr.total_amount,
    tr.product_id,
    tr.quantity,
    tr.item_price
FROM bronze.transaction tr
LEFT JOIN silver.dim_payment pa
    ON tr.payment_method = pa.payment_method 
    AND tr.payment_status = pa.payment_status
LEFT JOIN silver.dim_shipment sh
    ON tr.shipment_fee = sh.shipment_fee 
    AND tr.shipment_date_limit = sh.shipment_date_limit 
    AND tr.shipment_location_lat = sh.shipment_location_lat 
    AND tr.shipment_location_long = sh.shipment_location_long
LEFT JOIN silver.dim_promotion pr
    ON tr.promo_code = pr.promo_code 
    AND tr.promo_amount = pr.promo_amount;



