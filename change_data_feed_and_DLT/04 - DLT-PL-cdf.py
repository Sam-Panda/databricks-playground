# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr

# COMMAND ----------

folder_path = "/mnt/external_datalake/misc/change_data_feed1"

# COMMAND ----------

@dlt.view
def customer_bronze():
  df = spark.readStream.format("cloudFiles") \
      .option("cloudFiles.format", "csv") \
      .load(folder_path)
  return df


# COMMAND ----------

'''

Use the apply_changes() function in the Python API to use Delta Live Tables CDC functionality. The Delta Live Tables Python CDC interface also provides the create_streaming_live_table() function. You can use this function to create the target table required by the apply_changes() function.

'''


dlt.create_streaming_live_table(
  name = "customer_silver_scd_type_1",
  comment = " this is the silver data - SCD type 1",
  path = "/mnt/external_datalake/cdcdata/tables/customer_silver_scd_type_1",
    table_properties={
    "quality": "silver"
  }
)
  
dlt.apply_changes(
  target = "customer_silver_scd_type_1",
  source = "customer_bronze",
  keys = ["userid"],
  sequence_by = col("modifiedtime"),
  apply_as_deletes = expr("operation = 'DELETE'"),
  except_column_list = ["operation"],
  stored_as_scd_type = "1"
)

# COMMAND ----------

dlt.create_streaming_live_table(
  name = "customer_silver_scd_type_2",
  comment = " this is the silver data - SCD type 2",
  path = "/mnt/external_datalake/cdcdata/tables/customer_silver_scd_type_2",
    table_properties={
    "quality": "silver"
  }
)

dlt.apply_changes(
  target = "customer_silver_scd_type_2",
  source = "customer_bronze",
  keys = ["userid"],
  sequence_by = col("modifiedtime"),
  apply_as_deletes = expr("operation = 'DELETE'"),
  except_column_list = ["operation"],
  stored_as_scd_type = "2"
)
