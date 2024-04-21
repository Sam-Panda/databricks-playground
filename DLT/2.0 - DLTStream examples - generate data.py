# Databricks notebook source
# MAGIC %sql
# MAGIC USE catalog hive_metastore;
# MAGIC CREATE SCHEMA IF NOT EXISTS cdc_data;
# MAGIC
# MAGIC CREATE TABLE
# MAGIC   cdc_data.users
# MAGIC AS SELECT
# MAGIC   col1 AS userId,
# MAGIC   col2 AS name,
# MAGIC   col3 AS city,
# MAGIC   col4 AS operation,
# MAGIC   col5 AS sequenceNum
# MAGIC FROM (
# MAGIC   VALUES
# MAGIC   -- Initial load.
# MAGIC   (124, "Raul",     "Oaxaca",      "INSERT", 1),
# MAGIC   (123, "Isabel",   "Monterrey",   "INSERT", 1),
# MAGIC   -- New users.
# MAGIC   (125, "Mercedes", "Tijuana",     "INSERT", 2),
# MAGIC   (126, "Lily",     "Cancun",      "INSERT", 2),
# MAGIC   -- Isabel is removed from the system and Mercedes moved to Guadalajara.
# MAGIC   (123, null,       null,          "DELETE", 6),
# MAGIC   (125, "Mercedes", "Guadalajara", "UPDATE", 6),
# MAGIC   -- This batch of updates arrived out of order. The above batch at sequenceNum 5 will be the final state.
# MAGIC   (125, "Mercedes", "Mexicali",    "UPDATE", 5),
# MAGIC   (123, "Isabel",   "Chihuahua",   "UPDATE", 5)
# MAGIC   -- Uncomment to test TRUNCATE.
# MAGIC   -- ,(null, null,      null,          "TRUNCATE", 3)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC USE catalog hive_metastore;
# MAGIC CREATE SCHEMA IF NOT EXISTS cdc_data;
# MAGIC
# MAGIC CREATE TABLE
# MAGIC   cdc_data.users2
# MAGIC AS SELECT
# MAGIC   col1 AS userId,
# MAGIC   col2 AS name,
# MAGIC   col3 AS city,
# MAGIC   col4 AS sequenceNum
# MAGIC FROM (
# MAGIC   VALUES
# MAGIC   -- Initial load.
# MAGIC   (124, "Raul",     "Oaxaca",       1),
# MAGIC   (123, "Isabel",   "Monterrey",    1),
# MAGIC   -- New users.
# MAGIC   (125, "Mercedes", "Tijuana",     2),
# MAGIC   (126, "Lily",     "Cancun",      2)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- insert into cdc_data.users2 VALUES
# MAGIC --   (130, "Raul130",     "Oaxaca",       130);
# MAGIC Update cdc_data.users2
# MAGIC SET name = "Raul123"
# MAGIC where userid=123

# COMMAND ----------

# MAGIC %sql
# MAGIC USE catalog hive_metastore;
# MAGIC select * from cdc_data.users2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from hive_metastore.cdc_data.target
