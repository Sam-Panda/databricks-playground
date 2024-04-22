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
