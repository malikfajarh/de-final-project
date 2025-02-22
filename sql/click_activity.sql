CREATE SCHEMA IF NOT EXISTS stream;
CREATE TABLE IF NOT EXISTS stream.click_activity (
    session_id UUID,
    event_name VARCHAR,
    event_time TIMESTAMP,
    event_id UUID,
    traffic_source VARCHAR,
    product_id INT,
    quantity INT,
    item_price INT,
    payment_status VARCHAR,
    search_keywords TEXT,
    promo_code VARCHAR,
    promo_amount INT
);
