# Databricks notebook source
catalog = "workspace"

cdc_col = "Modified_Date"

backdated_refresh = ""

source_object = "silver_bookings"

source_schema = "silver"

#source fact table
fact_table = f"{catalog}.{source_schema}.{source_object}"

target_schema = "gold"

target_object = "FactBookings"

#fact Key columns
fact_key_col = ["DimCustomersKey","DimAirportsKey","DimFlightsKey","booking_id"]



# COMMAND ----------

# Create an array of all the dimensions that we want to pick
Dimensions = [

    {
        "table" : "workspace.gold.DimCustomers",
        "alias" : "DimCustomers",
        "join_keys" : [("passenger_id","passenger_id")]
    },

    {
        "table" : "workspace.gold.DimAirports",
        "alias" : "DimAirports",
        "join_keys" : [("airport_id","airport_id")]
    },

    {
        "table" : "workspace.gold.DimFlights",
        "alias" : "DimFlights",
        "join_keys" : [("flight_id","flight_id")]
    }
    
]

#fact columns we want to keep
fact_cols = ["amount","booking_id","booking_date","Modified_Date"]



# COMMAND ----------

#Last Load Date

if backdated_refresh == "":
    
    if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
        last_load = spark.sql(f"SELECT max({cdc_col} ) FROM workspace.{target_schema}.{target_object}").collect()[0][0]
    else:
        last_load = "1900-01-01 00:00:00"
else:
    last_load = backdated_refresh

# COMMAND ----------

# MAGIC %md
# MAGIC - the query we require to select the existing data from gold bookings table
# MAGIC >    Select amount,booking_id,booking_date,Modified_Date, DimCustomersKey, DimAirportsKey, DimFlightsKey from workspace.gold.FactBookings f Letf Join workspace.gold.DimCustomers c on f.passenger_id = c.passenger_id left Join workspace.gold.DimAirports a on f.airport_id = a.airport_id left Join workspace.gold.DimFlights fl on f.flight_id = fl.flight_id where Modified_Date > last_load  

# COMMAND ----------

def generate_fact_query_incremental(fact_cols, Dimensions, fact_table, last_load, cdc_col):
   
    
    fact_alias = "f"
    select_cols = [f'{fact_alias}.{col}' for col in fact_cols]

    join_cluases = []
    for dim in Dimensions:
        dim_alias = dim['alias']
        dim_table = dim['table']
        surrogate_key = f'{dim_alias}.{dim_alias}Key'
        select_cols.append(surrogate_key)
        
        on_clauses = [
            f'{dim_alias}.{join_key[0]} = {fact_alias}.{join_key[1]}' for join_key in dim['join_keys']
        ]
        
        join_cluase = f"LEFT JOIN {dim_table} {dim_alias} ON { ' AND '.join(on_clauses) }"
        
        join_cluases.append(join_cluase)


    select  = ", ".join(select_cols)
    join  = " ".join(join_cluases)

    where_cluase = f"{fact_alias}.{cdc_col} >= DATE('{last_load}')"

    fact_query = f"""SELECT {select} FROM {fact_table} {fact_alias} {join} WHERE {where_cluase} """.strip()

    return fact_query
        
    
        



    


# COMMAND ----------

query = generate_fact_query_incremental(fact_cols, Dimensions, fact_table, last_load, cdc_col)

# COMMAND ----------

query

# COMMAND ----------

df_fact = spark.sql(query)

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # UPSERT

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

merge_cond =  " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_col])

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}").alias("trg")\
    .merge(df_fact.alias("src"), f"{merge_cond}")\
    .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
    .whenNotMatchedInsertAll()\
    .execute()
else:
    df_fact.write.format("delta")\
        .mode("append")\
        .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

