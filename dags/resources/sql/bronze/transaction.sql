CREATE TABLE IF NOT EXISTS bronze.transaction (
    created_at TIMESTAMP,
    customer_id INT REFERENCES bronze.customer(customer_id),
    booking_id UUID,
    session_id UUID,
    payment_method VARCHAR,
    payment_status VARCHAR,
    promo_amount INT,
    promo_code VARCHAR,
    shipment_fee INT,
    shipment_date_limit TIMESTAMP,
    shipment_location_lat DOUBLE PRECISION,
    shipment_location_long DOUBLE PRECISION,
    total_amount INT,
    product_id INT REFERENCES bronze.product(id),
    quantity INT,
    item_price INT
);
