import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

@dlt.view(
    name = "trans_bookings"
)
def trans_bookings():
  df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
  df = df.withColumn("amount", df.amount.cast("double"))\
        .withColumn("Modified_Date", current_timestamp())\
        .withColumn("booking_date", to_date(df.booking_date))\
        .drop("_rescued_data")
  return df

#data quality checks using expectations
rules = {
    "rule1": "booking_id IS NOT NULL",          ## Rules Dictionary
    "rule2": "passenger_id IS NOT NULL"
}


@dlt.table(
    name = "silver_bookings"
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():
  return dlt.read_stream("trans_bookings")

#################################################################
# CDC for dimensions

@dlt.view(
    name = "trans_airports"
)
def trans_airports():
  df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
  df = df.drop("_rescued_data")\
        .withColumn("Modified_Date", current_timestamp())

  return df

dp.create_streaming_table("silver_airports")

dp.create_auto_cdc_flow(
  target = "silver_airports",
  source = "trans_airports",
  keys = ["airport_id"],
  sequence_by = col("Modified_Date"),
  stored_as_scd_type = 1
)


###############################################

@dlt.view(
    name = "trans_flights"
)
def trans_flights():
  df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/flights/data/")
  df = df.drop("_rescued_data")\
        .withColumn("flight_date", to_date(df.flight_date))\
        .withColumn("Modified_Date", current_timestamp())

  return df

dp.create_streaming_table("silver_flights")

dp.create_auto_cdc_flow(
  target = "silver_flights",
  source = "trans_flights",
  keys = ["flight_id"],
  sequence_by = col("Modified_Date"),
  stored_as_scd_type = 1
)

###################################################

@dlt.view(
    name = "trans_customers"
)
def trans_customers():
  df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/customers/data/")
  df = df.drop("_rescued_data")\
        .withColumn("Modified_Date", current_timestamp())

  return df

dp.create_streaming_table("silver_customers")

dp.create_auto_cdc_flow(
  target = "silver_customers",
  source = "trans_customers",
  keys = ["passenger_id"],
  sequence_by = col("Modified_Date"),
  stored_as_scd_type = 1
)

###############################################################
# Silver Business View
# creating materialized view

@dlt.materialized_view(
  name="silver_business_view"
)
def silver_business_view():
  return dlt.read("silver_bookings")\
            .join(dlt.read("silver_airports"), ["airport_id"])\
            .join(dlt.read("silver_customers"), ["passenger_id"])\
            .join(dlt.read("silver_flights"), ["flight_id"])\
            .drop("Modified_Date")
            
































