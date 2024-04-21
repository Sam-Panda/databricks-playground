# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr

# COMMAND ----------

@dlt.table
def change_data_feed_from_file():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .load("abfss://misc@adlsg2contosoodbk.dfs.core.windows.net/change_data_feed")
  )

# COMMAND ----------

dlt.create_streaming_table("target-from-cdf-feed")

dlt.apply_changes(
  target = "target",
  source = "change_data_feed_from_file",
  keys = ["userId"],
  sequence_by = col("modifiedtime"),
  apply_as_deletes = expr("operation = 'DELETE'"),
  except_column_list = ["operation", "modifiedtime"],
  stored_as_scd_type = "2",
  track_history_except_column_list = ["city"]
)
