# Databricks notebook source
bronze_df = spark.table("main.default.spotify_songs")

display(bronze_df)

# COMMAND ----------

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("main.default.bronze_spotify")

# COMMAND ----------

