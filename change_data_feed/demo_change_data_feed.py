# Databricks notebook source
# MAGIC %md
# MAGIC Creating the demo table for the change data feed example.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP  TABLE IF EXISTS cdc_data.users_change_data_feed
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS cdc_data;
# MAGIC create table if not exists cdc_data.users_change_data_feed
# MAGIC (
# MAGIC   userid int,
# MAGIC   name string, 
# MAGIC   city string, 
# MAGIC   modifiedtime timestamp
# MAGIC )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

import random
from datetime import datetime, timedelta

def generate_random_timestamp(start_date, end_date):
    start_datetime = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')

    start_timestamp = int(start_datetime.timestamp())
    end_timestamp = int(end_datetime.timestamp())
    
    random_timestamp = random.randint(start_timestamp, end_timestamp)
    generated_datetime = datetime.fromtimestamp(random_timestamp)
    
    return generated_datetime

# Example usage
start_date = '2022-01-01 00:00:00'
end_date = '2022-12-31 23:59:59'

random_timestamp = generate_random_timestamp(start_date, end_date)
print(random_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO cdc_data.users_change_data_feed
# MAGIC VALUES
# MAGIC   -- Initial load.
# MAGIC   (124, 'Raul', 'Oaxaca', '2022-08-04 20:44:23'),
# MAGIC   (123, 'Isabel', 'Monterrey', '2022-10-11 07:50:42'),
# MAGIC   -- New users.
# MAGIC   (125, 'Mercedes', 'Tijuana', '2022-01-14 16:36:13'),
# MAGIC   (126, 'Lily', 'Cancun', '2022-03-26 20:27:16')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('cdc_data.users_change_data_feed',0)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO cdc_data.users_change_data_feed
# MAGIC VALUES (130, 'Raul130', 'Oaxaca', current_timestamp());
# MAGIC
# MAGIC UPDATE cdc_data.users_change_data_feed
# MAGIC SET name = 'Raul123', modifiedtime=current_timestamp()
# MAGIC WHERE userid = 123;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('cdc_data.users_change_data_feed',1)
