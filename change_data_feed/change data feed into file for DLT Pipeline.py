# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- Create schema and control table for CDC data tracking
# MAGIC CREATE SCHEMA IF NOT EXISTS cdc_data;
# MAGIC
# MAGIC DROP TABLE IF EXISTS cdc_data.control_table;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS cdc_data.control_table (
# MAGIC     delta_table_name STRING,
# MAGIC     processed_begin_timestamp TIMESTAMP,
# MAGIC     processed_end_timestamp TIMESTAMP,
# MAGIC     current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC )
# MAGIC USING DELTA 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# MAGIC %sql
# MAGIC --Inserting the record in the control table for one of the delta table.
# MAGIC INSERT INTO cdc_data.control_table
# MAGIC VALUES ('cdc_data.users_change_data_feed', '2000-01-01T00:00:00', '2000-01-01T00:00:00', CURRENT_TIMESTAMP())

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from cdc_data.control_table

# COMMAND ----------

delta_table_name = 'cdc_data.users_change_data_feed'


# COMMAND ----------

# Query to fetch the processed_end_timestamp and assign it to a variable
processed_end_timestamp_query = f"SELECT processed_end_timestamp FROM cdc_data.control_table where delta_table_name = '{delta_table_name}'"
processed_end_timestamp_df = spark.sql(processed_end_timestamp_query)
processed_end_timestamp = processed_end_timestamp_df.collect()[0]["processed_end_timestamp"]
print(processed_end_timestamp)

# COMMAND ----------

query = f'''
SELECT userid, name, city, modifiedtime, 
CASE _change_type
    WHEN 'update_postimage' THEN 'update'
    ELSE _change_type
END AS operation
-- ,
-- _commit_version,
-- rank
FROM 
    (SELECT *, rank() over (partition by userid order by _commit_version desc) as rank
    FROM table_changes('cdc_data.users_change_data_feed', '{processed_end_timestamp}')
    WHERE _change_type != 'update_preimage')
WHERE 1=1
--and  rank = 1
order by modifiedtime desc
'''
df = spark.sql(query)

# COMMAND ----------

display(df)

# COMMAND ----------

import datetime
current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
# the below code content to create single csv file 
csv_temp_location = f"abfss://misc@adlsg2contosoodbk.dfs.core.windows.net/temp/change_data_feed/cdf_{delta_table_name}_{current_timestamp}.csv"
file_location = f"abfss://misc@adlsg2contosoodbk.dfs.core.windows.net/change_data_feed/cdf_{delta_table_name}_{current_timestamp}.csv"

df.repartition(1).write.csv(path=csv_temp_location, mode="append", header="true")

file = dbutils.fs.ls(csv_temp_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_temp_location, recurse=True)

# COMMAND ----------


