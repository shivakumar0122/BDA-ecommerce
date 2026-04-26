# Chapter 4 Evidence Pack

## Environment
- OS: Ubuntu 24.04 (WSL2)
- Java: 1.8.0_482
- Hadoop: 3.3.6
- Pig: 0.17.0
- Hive: 3.1.3
- Sqoop: 1.4.7
- Spark: 3.5.1 (local mode)

## Dataset
- Source: Olist Brazilian E-commerce CSV dataset (9 files)
- Local path: `/home/saimohaneesh/olist_data`

## HDFS Data Lake Verification
- Base path: `/user/saimohaneesh/ecommerce/`
- Raw zone size: `120.3 M`
- Cleaned zone size: `26.0 M`

## Pig Transformation Evidence
- Script: `/home/saimohaneesh/pig_scripts/clean_local_full.pig`
- Output local: `/home/saimohaneesh/pig_output/part-r-00000`
- Output rows: `110197`
- Output pushed to HDFS: `/user/saimohaneesh/ecommerce/cleaned/part-r-00000`

## Sqoop Ingestion Evidence

### MySQL Source (user-space instance)
- Config: `/home/saimohaneesh/mysql-local/my.cnf`
- Socket: `/home/saimohaneesh/mysql-local/run/mysql.sock`
- Port: `3307`
- Database: `olist_db`

### Imported tables (Sqoop -> HDFS)
1. `orders_sqoop`
   - HDFS target: `/user/saimohaneesh/ecommerce/raw_sqoop/orders_sqoop`
   - Records imported: `99441`
   - Size: `17.6 M`

2. `payments_sqoop`
   - HDFS target: `/user/saimohaneesh/ecommerce/raw_sqoop/payments_sqoop`
   - Records imported: `103886`
   - Size: `5.4 M`

## Hive Evidence
- History file: `/home/saimohaneesh/.hivehistory`
- Table context: `ecommerce_db.ecommerce_analytics`
- Query evidence includes successful aggregate runs and top-product results.

## Spark Analytics Evidence
- Script: `/home/saimohaneesh/bda-ecommerce/scripts/spark_analytics.py`
- Run command:
  - `spark-submit --master local[*] /home/saimohaneesh/bda-ecommerce/scripts/spark_analytics.py`
- Output folder: `/home/saimohaneesh/bda-ecommerce/results/spark`

### Generated outputs
1. `top_products.csv`
2. `monthly_trend.csv`
3. `state_revenue.csv`
4. `delivery_summary.csv`
5. `category_review.csv`

## Suggested Figure Mapping (for Report)
1. `jps` output showing 5 daemons
2. NameNode UI (`http://localhost:9870`)
3. YARN UI (`http://localhost:8088`)
4. HDFS raw/cleaned directory listing
5. Pig output (`hdfs dfs -ls /user/saimohaneesh/ecommerce/cleaned/`)
6. Sqoop import success log (`Retrieved 99441 records`)
7. Sqoop HDFS target listing
8. Hive query output screenshot
9. Spark submit completion (`Spark analytics completed`)
10. CSV result snapshots from `/home/saimohaneesh/bda-ecommerce/results/spark`
