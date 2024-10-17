# Databricks notebook source
data=[(1,'a',20),(2,'b',30)]



# COMMAND ----------


schema="id int, name string, age int"
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

from pyspark.sql.functions import*

# COMMAND ----------

df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

df.withColumnRenamed("id", "emp_id").display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df.withColumnRenamed({"id":"emp_id","name":"emp_name","age":"emp_age"})

# COMMAND ----------

df.withColumn("current_date", current_date()).display()

# COMMAND ----------


