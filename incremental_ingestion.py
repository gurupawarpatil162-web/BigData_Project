# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

source_df = spark.table("main.default.spotify_songs")

# COMMAND ----------

source_df = source_df.withColumn(
    "row_id",
    monotonically_increasing_id()
)

# COMMAND ----------

# metadata = [(1,)]

# metadata_df = spark.createDataFrame(
#     metadata,
#     ["last_row"]
# )

# metadata_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .saveAsTable("main.default.spotify_ingestion_metadata")

# COMMAND ----------

metadata_df = spark.table(
    "main.default.spotify_ingestion_metadata"
)

last_row = metadata_df.collect()[0]["last_row"]

print("Last Processed Row:", last_row)

# COMMAND ----------

batch_size = 10000

# COMMAND ----------

new_batch_df = source_df.filter(
    (col("row_id") > last_row)
).limit(batch_size)

# COMMAND ----------

new_batch_df = new_batch_df.withColumn(
    "track_popularity",
    (
        col("track_popularity")
        + (rand() * 5)
    ).cast("bigint")
)

# COMMAND ----------

new_batch_df = new_batch_df.withColumn(
    "ingestion_time",
    current_timestamp()
)

# COMMAND ----------

new_batch_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("main.default.bronze_spotify")

# COMMAND ----------

max_row = new_batch_df.agg(
    max("row_id")
).collect()[0][0]

updated_metadata = [(max_row,)]

updated_metadata_df = spark.createDataFrame(
    updated_metadata,
    ["last_row"]
)

updated_metadata_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("main.default.spotify_ingestion_metadata")

# COMMAND ----------

