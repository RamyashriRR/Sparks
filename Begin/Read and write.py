# Databricks notebook source
df=spark.read.csv("/Volumes/my_databricks/default/raw/sales.csv",header=True,inferSchema=True)

# COMMAND ----------

from pyspark.sql.functions import *
df = spark.read.csv("/Volumes/my_databricks/default/raw/sales.csv",header=True,inferSchema=True)
df.write.mode("overwrite").saveAsTable("sales")
df_json = spark.read.json("/Volumes/my_databricks/default/raw/customers.json")
df_json.write.saveAsTable("customers")

# COMMAND ----------


