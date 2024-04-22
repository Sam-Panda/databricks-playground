# Databricks notebook source
delta_table_name = 'cdc_data.users_change_data_feed'


# COMMAND ----------

# Query to fetch the processed_end_timestamp and assign it to a variable
query = f"SELECT processed_end_timestamp, change_data_feed_file_name FROM cdc_data.control_table where delta_table_name = '{delta_table_name}'"
df = spark.sql(query)
processed_end_timestamp = df.collect()[0]["processed_end_timestamp"]
change_data_feed_file_name = df.collect()[0]["change_data_feed_file_name"]

change_data_feed_file_name_prefix_words = change_data_feed_file_name.split(".")[0].split("_")
change_data_feed_file_name_prefix_words.pop()
change_data_feed_file_name_prefix='_'.join(change_data_feed_file_name_prefix_words)

change_data_feed_file_number = str(int(change_data_feed_file_name.split("_")[-1].split(".")[0])+1).zfill(9)
new_file_name = change_data_feed_file_name_prefix+"_"+change_data_feed_file_number+".csv"
print(processed_end_timestamp)
print(new_file_name)

# COMMAND ----------

query = f'''
SELECT userid, name, city, modifiedtime, 
CASE _change_type
    WHEN 'update_postimage' THEN 'update'
    ELSE _change_type
END AS operation
FROM 
    (SELECT *
    FROM table_changes('cdc_data.users_change_data_feed', '{processed_end_timestamp}')
    WHERE _change_type != 'update_preimage')
order by modifiedtime desc
'''
df = spark.sql(query)

# COMMAND ----------

display(df)

# COMMAND ----------

import datetime
current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
# the below code content to create single csv file 
csv_temp_location = f"abfss://misc@adlsg2contosoodbk.dfs.core.windows.net/temp/change_data_feed1/{new_file_name}"
file_location = f"abfss://misc@adlsg2contosoodbk.dfs.core.windows.net/change_data_feed1/{new_file_name}"

df.repartition(1).write.csv(path=csv_temp_location, mode="append", header="true")

file = dbutils.fs.ls(csv_temp_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_temp_location, recurse=True)

# COMMAND ----------

current_timestamp = datetime.datetime.now()
sqlquery = f'''
update cdc_data.control_table 
set processed_end_timestamp='{current_timestamp}', change_data_feed_file_name='{new_file_name}', current_timestamp='{current_timestamp}'
where delta_table_name = '{delta_table_name}'
'''
spark.sql(sqlquery)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from cdc_data.control_table
