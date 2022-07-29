# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Sample data
# MAGIC 
# MAGIC Create Databricks tables jaffle_shop.orders, jaffle_shop.customers, and stripe.payments from these CSV files, which are located in a public S3 bucket (docs):
# MAGIC 
# MAGIC ```
# MAGIC orders: http://dbt-tutorial-public.s3-us-west-2.amazonaws.com/jaffle_shop_orders.csv
# MAGIC customers: http://dbt-tutorial-public.s3-us-west-2.amazonaws.com/jaffle_shop_customers.csv
# MAGIC payments: http://dbt-tutorial-public.s3-us-west-2.amazonaws.com/stripe_payments.csv
# MAGIC ```

# COMMAND ----------

!mkdir /dbfs/tmp/jaffle && \
wget http://dbt-tutorial-public.s3-us-west-2.amazonaws.com/jaffle_shop_orders.csv -O /dbfs/tmp/jaffle/orders.csv && \
wget http://dbt-tutorial-public.s3-us-west-2.amazonaws.com/jaffle_shop_customers.csv -O /dbfs/tmp/jaffle/customers.csv && \
wget http://dbt-tutorial-public.s3-us-west-2.amazonaws.com/stripe_payments.csv -O /dbfs/tmp/jaffle/payments.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS jaffle_shop;
# MAGIC CREATE DATABASE IF NOT EXISTS stripe;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS jaffle_shop.customers;
# MAGIC DROP TABLE IF EXISTS stripe.payments;
# MAGIC DROP TABLE IF EXISTS jaffle_shop.orders

# COMMAND ----------

df_customers = spark.read.option("header", True).option("inferSchema", True).csv("/tmp/jaffle/customers.csv")
df_orders = spark.read.option("header", True).option("inferSchema", True).csv("/tmp/jaffle/orders.csv")
df_payments = spark.read.option("header", True).option("inferSchema", True).csv("/tmp/jaffle/payments.csv")

df_customers.write.saveAsTable(name = "jaffle_shop.customers")
df_orders.write.saveAsTable(name = "jaffle_shop.orders")
df_payments.write.saveAsTable(name = "stripe.payments")
