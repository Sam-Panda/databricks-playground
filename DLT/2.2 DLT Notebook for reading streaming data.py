# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr

@dlt.view
def users():
  return spark.readStream.format("delta").table("hive_metastore.cdc_data.users2")

dlt.create_streaming_table("target2")

dlt.apply_changes(
  target = "target2",
  source = "users",
  keys = ["userId"],
  sequence_by = col("sequenceNum"),
  except_column_list = ["sequenceNum"],
  stored_as_scd_type = "2",
  track_history_except_column_list = ["city"]
)
