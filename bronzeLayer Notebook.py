# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Incremental Data Ingestion

# COMMAND ----------

dbutils.widgets.text("src", "")
src_value = dbutils.widgets.get("src")

# COMMAND ----------

display(src_value)

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option("cloudFiles.schemaLocation", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint/")\
        .option("cloudFiles.schemaEvolutionMode", "rescue")\
        .load(f"/Volumes/workspace/raw/rawvolume/rawdata/{src_value}/")

# COMMAND ----------

df.writeStream.format("delta")\
    .option("outputMode", "append")\
    .option("checkpointLocation", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint")\
    .trigger(once=True)\
    .option("path",f"/Volumes/workspace/bronze/bronzevolume/{src_value}/data")\
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Delta.`/Volumes/workspace/bronze/bronzevolume/customers/data`

# COMMAND ----------

