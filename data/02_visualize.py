# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Staging

# COMMAND ----------

# DBTITLE 1,Customers
# MAGIC %sql
# MAGIC 
# MAGIC select * from dev_dbt_rafael.stg_jaffle_shop_customers

# COMMAND ----------

# DBTITLE 1,Orders
# MAGIC %sql
# MAGIC 
# MAGIC select * from dev_dbt_rafael.stg_jaffle_shop_orders

# COMMAND ----------

# DBTITLE 1,Payments
# MAGIC %sql
# MAGIC 
# MAGIC select * from dev_dbt_rafael.stg_stripe_payments

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Fact Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dev_dbt_rafael.fct_orders

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Dimensions

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dev_dbt_rafael.dim_customers
