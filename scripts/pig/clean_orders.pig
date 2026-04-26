-- Load raw orders (CSV has quoted fields)
raw_orders = LOAD '/user/saimohaneesh/ecommerce/raw/orders'
    USING PigStorage(',')
    AS (order_id:chararray, customer_id:chararray, order_status:chararray,
        purchase_ts:chararray, approved_at:chararray,
        delivered_carrier:chararray, delivered_customer:chararray,
        estimated_delivery:chararray);

-- Load order items
raw_items = LOAD '/user/saimohaneesh/ecommerce/raw/order_items'
    USING PigStorage(',')
    AS (order_id:chararray, item_id:chararray, product_id:chararray,
        seller_id:chararray, ship_limit:chararray,
        price:chararray, freight:chararray);

-- Load customers
raw_customers = LOAD '/user/saimohaneesh/ecommerce/raw/customers'
    USING PigStorage(',')
    AS (customer_id:chararray, unique_id:chararray,
        zip_prefix:chararray, city:chararray, state:chararray);

-- Load products
raw_products = LOAD '/user/saimohaneesh/ecommerce/raw/products'
    USING PigStorage(',')
    AS (product_id:chararray, category_name:chararray,
        name_len:chararray, desc_len:chararray, photos_qty:chararray,
        weight_g:chararray, length_cm:chararray,
        height_cm:chararray, width_cm:chararray);

-- Skip header rows (headers have quotes around them)
orders_noheader = FILTER raw_orders BY
    NOT (order_id MATCHES '.*order_id.*');

items_noheader = FILTER raw_items BY
    NOT (order_id MATCHES '.*order_id.*');

customers_noheader = FILTER raw_customers BY
    NOT (customer_id MATCHES '.*customer_id.*');

products_noheader = FILTER raw_products BY
    NOT (product_id MATCHES '.*product_id.*');

-- Filter delivered orders only, remove nulls
delivered = FILTER orders_noheader BY
    order_status == 'delivered'
    AND order_id IS NOT NULL
    AND customer_id IS NOT NULL;

-- Remove duplicates
deduped = DISTINCT delivered;

-- Filter valid items
valid_items = FILTER items_noheader BY
    order_id IS NOT NULL
    AND product_id IS NOT NULL
    AND price IS NOT NULL;

-- Join orders with items
orders_items = JOIN deduped BY order_id, valid_items BY order_id;

-- Join with customers
orders_items_customers = JOIN orders_items BY deduped::customer_id,
    customers_noheader BY customer_id;

-- Join with products
full_data = JOIN orders_items_customers
    BY valid_items::product_id LEFT OUTER,
    products_noheader BY product_id;

-- Generate clean output with derived fields
clean_output = FOREACH full_data GENERATE
    deduped::order_id                       AS order_id,
    deduped::customer_id                    AS customer_id,
    customers_noheader::state               AS customer_state,
    customers_noheader::city                AS customer_city,
    valid_items::product_id                 AS product_id,
    products_noheader::category_name        AS category_name,
    valid_items::seller_id                  AS seller_id,
    valid_items::price                      AS price,
    valid_items::freight                    AS freight_value,
    deduped::purchase_ts                    AS purchase_timestamp,
    SUBSTRING(deduped::purchase_ts, 0, 4)   AS order_year,
    SUBSTRING(deduped::purchase_ts, 5, 7)   AS order_month,
    deduped::estimated_delivery             AS estimated_delivery,
    deduped::delivered_customer             AS actual_delivery;

-- Store cleaned data to HDFS
STORE clean_output INTO '/user/saimohaneesh/ecommerce/cleaned'
    USING PigStorage('\t');
