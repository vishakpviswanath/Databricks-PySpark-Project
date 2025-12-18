# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Transformations

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format('delta')\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Cleansing
# MAGIC - Removing duplicates
# MAGIC - type conversion
# MAGIC - removing rescued data column
# MAGIC - Adding new column for processed timestamp

# COMMAND ----------

df = df.withColumn("amount", df.amount.cast("double"))\
        .withColumn("Modified_Date", current_timestamp())\
        .withColumn("booking_date", to_date(df.booking_date))\
        .drop("_rescued_data")



display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Delta.`/Volumes/workspace/bronze/bronzevolume/customers/data/`

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from workspace.silver.silver_business_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.silver_airports

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.silver_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from workspace.gold.dimcustomers

# COMMAND ----------

