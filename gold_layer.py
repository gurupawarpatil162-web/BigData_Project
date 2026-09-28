# Databricks notebook source
silver_df = spark.table("main.default.silver_spotify")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

artist_gold_df = silver_df.groupBy("track_artist") \
    .agg(
        avg("track_popularity").alias("avg_popularity"),
        avg("danceability").alias("avg_danceability"),
        avg("energy").alias("avg_energy")
    )

# COMMAND ----------

genre_gold_df = silver_df.groupBy("playlist_genre") \
    .agg(
        avg("track_popularity").alias("avg_popularity"),
        avg("energy").alias("avg_energy")
    )

# COMMAND ----------

album_gold_df = silver_df.groupBy("track_album_name") \
    .agg(
        avg("track_popularity").alias("avg_popularity")
    )

# COMMAND ----------

artist_gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("main.default.gold_artist_analytics")

# COMMAND ----------

genre_gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("main.default.gold_genre_analytics")

# COMMAND ----------

album_gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("main.default.gold_album_analytics")

# COMMAND ----------

