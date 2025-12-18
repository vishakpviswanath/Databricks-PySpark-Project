# Databricks notebook source
# MAGIC %md
# MAGIC - Implementing SCD Type 1 to create DIM tables
# MAGIC - Dynamic Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# catalog = "workspace"

# key_cols =  "['flight_id']"
# key_cols_list = eval(key_cols)

# cdc_col = "Modified_Date"

# backdated_refresh = ""

# source_object = "silver_flights"

# source_schema = "silver"

# target_schema = "gold"

# target_object = "DIMflights"

# surrogate_key = "DimFlightsKey"


# COMMAND ----------

catalog = "workspace"

key_cols =  "['passenger_id']"
key_cols_list = eval(key_cols)

cdc_col = "Modified_Date"

backdated_refresh = ""

source_object = "silver_customers"

source_schema = "silver"

target_schema = "gold"

target_object = "DIMCustomers"

surrogate_key = "DimCustomersKey"

# COMMAND ----------

if backdated_refresh == "":
    
    if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
        last_load = spark.sql(f"SELECT max({cdc_col} ) FROM workspace.{target_schema}.{target_object}").collect()[0][0]
    else:
        last_load = "1900-01-01 00:00:00"
else:
    last_load = backdated_refresh
    

# COMMAND ----------

last_load

# COMMAND ----------

df_src = spark.sql(f"SELECT * FROM {catalog}.{source_schema}.{source_object} WHERE {cdc_col} >= '{last_load}'")

# COMMAND ----------

display(df_src)

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ## Target Table Processing
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create target table if not exists

# COMMAND ----------


### Create target table if not exists 


##If table exists, then get the required columns from the target tables

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    key_cols_str_incremental = ', '.join(key_cols_list)
    df_trg = spark.sql(f"SELECT {key_cols_str_incremental}, {surrogate_key}, create_date, update_date FROM {catalog}.{target_schema}.{target_object}")

## If table doesn't exist, then create the table with required columns
else:
    # Key_cols_str_init is used to make the notebook dynamic. o/p will be '' AS flight_id, '' AS destination etc.
    key_cols_str_init = ', '.join(["'' AS " + i for i in key_cols_list])
    df_trg = spark.sql(f"SELECT {key_cols_str_init}, cast('0' as bigint) AS {surrogate_key}, cast('1900-01-01 00:00:00' as timestamp) AS create_date, cast('1900-01-01 00:00:00' as timestamp) AS update_date WHERE 1= 0")


# COMMAND ----------


display(df_trg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1 of creating the target table: Adding Surrogate Key and Other Required Fields

# COMMAND ----------


## defining the join condition based on the key columns. Dynamic solution
join_condition = 'AND '.join([f"src.{i} = trg.{i}" for i in key_cols_list])


# COMMAND ----------


### Converting the dataframes to temp views for joining. Joining the source and target tables to see if we have new records or did we process the existing records. 

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")


df_join = spark.sql(
            f"""
            SELECT src.*,
            trg.{surrogate_key},
            trg.create_date,
            trg.update_date
            FROM src
            LEFT JOIN trg
            ON {join_condition}
        """
        )


# COMMAND ----------

display(df_join)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_old = df_join.filter(col(f'{surrogate_key}').isNotNull()) # Records which are already present in the target table
df_new = df_join.filter(col(f'{surrogate_key}').isNull()) # Records which are not present in the target table

# COMMAND ----------

display(df_old)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing existing old records.

# COMMAND ----------

#Updating the update date for the records which are already present in the target table
df_old_enr = df_old.withColumn('update_date', current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing the new records

# COMMAND ----------

###########Inserting new records in the target table
## If we already have the table, we will get the max surrogate key and increment it by 1 to start the new records
if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    max_surrogate_key = spark.sql( f"""
                                  select max({surrogate_key}) from {catalog}.{target_schema}.{target_object}
                                """).collect()[0][0]
    
    df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                .withColumn('create_date', current_timestamp())\
                .withColumn('update_date', current_timestamp())
# If we don't have the table, we will start the surrogate key from 1
else:
    max_surrogate_key = 0
    df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                .withColumn('create_date', current_timestamp())\
                .withColumn('update_date', current_timestamp())


# COMMAND ----------

df_new_enr.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unioning the old updated records and new processed records

# COMMAND ----------

df_union = df_old_enr.unionByName(df_new_enr)
df_union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # UPSERT - SCD TYPE 1

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}").alias("trg")\
    .merge(df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}")\
    .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
    .whenNotMatchedInsertAll()\
    .execute()
else:
    df_union.write.format("delta")\
        .mode("append")\
        .saveAsTable(f"{catalog}.{target_schema}.{target_object}")