# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE table customers as 
# MAGIC  SELECT *, current_timestamp() as ingestion_date from json.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.'/Volumes/my_databricks/default/raw/customers.json'

# COMMAND ----------


