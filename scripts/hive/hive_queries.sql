CREATE DATABASE IF NOT EXISTS ecommerce_db;
USE ecommerce_db;

CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_analytics (
    order_id           STRING,
    customer_id        STRING,
    customer_state     STRING,
    customer_city      STRING,
    product_id         STRING,
    category_name      STRING,
    seller_id          STRING,
    price              STRING,
    freight_value      STRING,
    purchase_timestamp STRING,
    order_year         STRING,
    order_month        STRING,
    estimated_delivery STRING,
    actual_delivery    STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/saimohaneesh/ecommerce/cleaned';

SET hive.stats.autogather=false;
SET hive.exec.mode.local.auto=true;
SET mapreduce.map.memory.mb=2048;
SET mapreduce.map.java.opts=-Xmx1600m;
SET mapreduce.reduce.memory.mb=2048;
SET mapreduce.reduce.java.opts=-Xmx1600m;
SET io.sort.mb=256;

SELECT COUNT(*) FROM ecommerce_analytics;

SELECT
    product_id,
    category_name,
    ROUND(SUM(CAST(price AS DOUBLE)), 2) AS total_revenue,
    COUNT(order_id) AS units_sold
FROM ecommerce_analytics
WHERE product_id IS NOT NULL
  AND price IS NOT NULL
GROUP BY product_id, category_name
ORDER BY total_revenue DESC
LIMIT 10;
