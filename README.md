# rainfall Analytics Pipeline

This repository contains the implementation artifacts for a Big Data Analytics project using Hadoop ecosystem tools on the Olist e-commerce dataset.

## Stack
- Hadoop 3.3.6 (HDFS + YARN)
- Pig 0.17.0
- Hive 3.1.3
- Sqoop 1.4.7
- Spark 3.5.1

## Project Layout
- `scripts/pig/` Pig transformation scripts
- `scripts/hive/` Hive table/query script
- `scripts/sqoop/` Sqoop import commands
- `scripts/spark_analytics.py` Spark analytics job
- `configs/hadoop/` Hadoop config snapshots
- `configs/hive/` Hive config snapshot
- `results/spark/` Spark result CSV files
- `docs/CHAPTER4_EVIDENCE.md` Chapter 4 execution evidence

## Key Outputs
- HDFS cleaned dataset: `/user/saimohaneesh/ecommerce/cleaned/part-r-00000`
- Sqoop-imported datasets:
  - `/user/saimohaneesh/ecommerce/raw_sqoop/orders_sqoop`
  - `/user/saimohaneesh/ecommerce/raw_sqoop/payments_sqoop`
- Spark result files under `results/spark/`

## Run Spark Analytics
```bash
export SPARK_HOME=/home/saimohaneesh/spark-3.5.1-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin
spark-submit --master local[*] /home/saimohaneesh/bda-ecommerce/scripts/spark_analytics.py
```

## Notes
- The MySQL source used by Sqoop is a user-space local MySQL instance (port `3307`) configured under `/home/saimohaneesh/mysql-local`.
- Use screenshots from terminal and web UIs (`9870`, `8088`) for report Chapter 4 figures.
