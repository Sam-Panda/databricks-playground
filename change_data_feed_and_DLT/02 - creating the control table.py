# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- Create schema and control table for CDC data tracking
# MAGIC CREATE SCHEMA IF NOT EXISTS cdc_data;
# MAGIC
# MAGIC --DROP TABLE IF EXISTS cdc_data.control_table;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS cdc_data.control_table (
# MAGIC     delta_table_name STRING,   
# MAGIC     processed_end_timestamp TIMESTAMP,
# MAGIC     change_data_feed_file_name STRING,
# MAGIC     current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC )
# MAGIC USING DELTA 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Insert a record into the control table for the delta table related to user changes.
# MAGIC INSERT INTO cdc_data.control_table (
# MAGIC     delta_table_name,   
# MAGIC     processed_end_timestamp,
# MAGIC     change_data_feed_file_name,
# MAGIC     current_timestamp
# MAGIC )
# MAGIC VALUES (
# MAGIC     'cdc_data.users_change_data_feed',
# MAGIC     '2000-01-01T00:00:00', -- inserting old value as we are initiating the control table.
# MAGIC     'cdf_cdc_data_users_change_data_feed_000000000.csv', -- adding a numeric value at the end for Lexical ordering (https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/directory-listing-mode#--lexical-ordering-of-files)
# MAGIC      CURRENT_TIMESTAMP() 
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from cdc_data.control_table
