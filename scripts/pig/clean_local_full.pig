-- Load from local filesystem
raw_orders = LOAD '/home/saimohaneesh/olist_data/olist_orders_dataset.csv'
    USING PigStorage(',')
    AS (order_id:chararray, customer_id:chararray, order_status:chararray,
        purchase_ts:chararray, approved_at:chararray,
        delivered_carrier:chararray, delivered_customer:chararray,
        estimated_delivery:chararray);

raw_items = LOAD '/home/saimohaneesh/olist_data/olist_order_items_dataset.csv'
    USING PigStorage(',')
    AS (order_id:chararray, item_id:chararray, product_id:chararray,
        seller_id:chararray, ship_limit:chararray,
        price:chararray, freight:chararray);

raw_customers = LOAD '/home/saimohaneesh/olist_data/olist_customers_dataset.csv'
    USING PigStorage(',')
    AS (customer_id:chararray, unique_id:chararray,
        zip_prefix:chararray, city:chararray, state:chararray);

raw_products = LOAD '/home/saimohaneesh/olist_data/olist_products_dataset.csv'
    USING PigStorage(',')
    AS (product_id:chararray, category_name:chararray,
        name_len:chararray, desc_len:chararray, photos_qty:chararray,
        weight_g:chararray, length_cm:chararray,
        height_cm:chararray, width_cm:chararray);

-- Skip headers
orders_noheader = FILTER raw_orders BY
    NOT (order_id MATCHES '.*order_id.*');
items_noheader = FILTER raw_items BY
    NOT (order_id MATCHES '.*order_id.*');
customers_noheader = FILTER raw_customers BY
    NOT (customer_id MATCHES '.*customer_id.*');
products_noheader = FILTER raw_products BY
    NOT (product_id MATCHES '.*product_id.*');

-- Filter delivered orders
delivered = FILTER orders_noheader BY
    order_status == 'delivered'
    AND order_id IS NOT NULL
    AND customer_id IS NOT NULL;

deduped = DISTINCT delivered;

-- Filter valid items
valid_items = FILTER items_noheader BY
    order_id IS NOT NULL
    AND product_id IS NOT NULL
    AND price IS NOT NULL;

-- Join all tables
orders_items = JOIN deduped BY order_id, valid_items BY order_id;
orders_items_customers = JOIN orders_items BY deduped::customer_id,
    customers_noheader BY customer_id;
full_data = JOIN orders_items_customers
    BY valid_items::product_id LEFT OUTER,
    products_noheader BY product_id;

-- Generate clean output
clean_output = FOREACH full_data GENERATE
    deduped::order_id                     AS order_id,
    deduped::customer_id                  AS customer_id,
    customers_noheader::state             AS customer_state,
    customers_noheader::city              AS customer_city,
    valid_items::product_id               AS product_id,
    products_noheader::category_name      AS category_name,
    valid_items::seller_id                AS seller_id,
    valid_items::price                    AS price,
    valid_items::freight                  AS freight_value,
    deduped::purchase_ts                  AS purchase_timestamp,
    SUBSTRING(deduped::purchase_ts, 0, 4) AS order_year,
    SUBSTRING(deduped::purchase_ts, 5, 7) AS order_month,
    deduped::estimated_delivery           AS estimated_delivery,
    deduped::delivered_customer           AS actual_delivery;

-- Store output locally first
STORE clean_output INTO '/home/saimohaneesh/pig_output'
    USING PigStorage('\t');
