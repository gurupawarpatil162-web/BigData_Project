# Databricks notebook source
bronze_df = spark.table("main.default.bronze_spotify")

# COMMAND ----------

from pyspark.sql.functions import *

silver_df = bronze_df \
    .dropDuplicates() \
    .na.drop()

# COMMAND ----------

silver_df = silver_df.withColumn(
    "duration_minutes",
    round(col("duration_ms") / 60000, 2)
)

# COMMAND ----------

silver_df = silver_df.withColumn(
    "popularity_category",
    when(col("track_popularity") >= 80, "Hit")
    .when(col("track_popularity") >= 50, "Popular")
    .otherwise("Normal")
)

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("main.default.silver_spotify")

# COMMAND ----------

