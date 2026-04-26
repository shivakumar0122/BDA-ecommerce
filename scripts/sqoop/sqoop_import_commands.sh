#!/usr/bin/env bash
set -e

export SQOOP_HOME=/home/saimohaneesh/sqoop-1.4.7.bin__hadoop-2.6.0
export PATH=$PATH:$SQOOP_HOME/bin

# Orders import
hdfs dfs -rm -r -f /user/saimohaneesh/ecommerce/raw_sqoop/orders_sqoop >/dev/null 2>&1 || true
sqoop import \
  --connect "jdbc:mysql://127.0.0.1:3307/olist_db?useSSL=false" \
  --username sqoop_user \
  --password sqoop123 \
  --table orders_sqoop \
  --target-dir /user/saimohaneesh/ecommerce/raw_sqoop/orders_sqoop \
  --num-mappers 1 \
  --as-textfile

# Payments import
hdfs dfs -rm -r -f /user/saimohaneesh/ecommerce/raw_sqoop/payments_sqoop >/dev/null 2>&1 || true
sqoop import \
  --connect "jdbc:mysql://127.0.0.1:3307/olist_db?useSSL=false" \
  --username sqoop_user \
  --password sqoop123 \
  --table payments_sqoop \
  --target-dir /user/saimohaneesh/ecommerce/raw_sqoop/payments_sqoop \
  --num-mappers 1 \
  --as-textfile

hdfs dfs -ls /user/saimohaneesh/ecommerce/raw_sqoop/orders_sqoop
hdfs dfs -ls /user/saimohaneesh/ecommerce/raw_sqoop/payments_sqoop
