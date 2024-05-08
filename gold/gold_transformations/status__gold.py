# Databricks notebook source
dbutils.widgets.text(name = "entity", defaultValue = "", label = "Entity")

# COMMAND ----------

entity = dbutils.widgets.get("entity")

# COMMAND ----------

from f1_schemas import *

myent = F1_Schemas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create delta table, transform it to gold and save it in HIVE and BLOB

# COMMAND ----------

attributes = myent.entities[entity]
# create gold delta table
# join the list of columns to be read by databricks in the create table sql statement
spark.sql(f"""CREATE TABLE IF NOT EXISTS {attributes["gold_name"]} (
    {
        ", ".join(
            ["".join(k)+ " " + "".join(v) for k,v in attributes['schema'].items()]
        )
    })
    USING DELTA
    OPTIONS (path '{attributes["hive_metastore_gold_path"]}')
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)
    

print(f"""Processing {attributes['blob_silver_path']} to save history of delta table on {attributes['gold_name']} and updating last parquet in Blob Storage {attributes["blob_gold_path"]}""")

# CREATE GOLD LAYER

# Read silver last version of table
df = spark.table(f'{attributes["silver_name"]}')

actual_delta_df = spark.table(f'{attributes["gold_name"]}') # Read the latest version of the gold Delta table
new_records_df = df.exceptAll(actual_delta_df) # Keep just new records comparing the new df against actual version of delta table
new_records_df.write.format("delta").mode("append").saveAsTable(f'{attributes["gold_name"]}') # Append just new records to delta table
df.write.format("parquet").mode("overwrite").save(f'{attributes["blob_gold_path"]}') # overwrite last version in Blob storage with new version of delta table as standard parquet file
print(f'File {attributes["blob_gold_path"]} saved successfully.')


# COMMAND ----------

# exceptAll doesn't validate for duplicates so is faster than subtract that does remove duplicates 
# %timeit df.exceptAll(actual_delta_df)
# %timeit df.subtract(actual_delta_df)

# COMMAND ----------

# from delta.tables import *

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {attributes['gold_name']}"))

