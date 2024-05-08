# Databricks notebook source
dbutils.widgets.text(name = "entity", defaultValue = "", label = "Entity")
entity = dbutils.widgets.get("entity")

# COMMAND ----------

from f1_schemas import *

myent = F1_Schemas()
myent.entities[entity]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create delta table, transform it to silver and save it in HIVE and BLOB

# COMMAND ----------

attributes = myent.entities[entity]
# create silver delta table
# join the list of columns to be read by databricks in the create table sql statement
spark.sql(f"""CREATE TABLE IF NOT EXISTS {attributes["silver_name"]} (
    {
        ", ".join(
            ["".join(k)+ " " + "".join(v) for k,v in attributes['schema'].items()]
        )
    })
    USING DELTA
    OPTIONS (path '{attributes["hive_metastore_silver_path"]}')
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)
    

print(f"""Processing {attributes['blob_bronce_path']} to save history of delta table on {attributes['silver_name']} and updating last parquet in Blob Storage {attributes["blob_silver_path"]}""")

# CREATE SILVER LAYER

# Read bronce csv file
df = spark.read.format("csv").option("delimiter",",").option("header",True).option("inferSchema",True).load(attributes["blob_bronce_path"])

actual_delta_df = spark.table(f'{attributes["silver_name"]}') # Read the latest version of the Silver Delta table
new_records_df = df.exceptAll(actual_delta_df) # Keep just new records comparing the new df against actual version of delta table
new_records_df.write.format("delta").mode("append").saveAsTable(f'{attributes["silver_name"]}') # Append just new records to delta table
df.write.format("parquet").mode("overwrite").save(f'{attributes["blob_silver_path"]}') # overwrite last version in Blob storage with new version of delta table as standard parquet file
print(f'File {attributes["blob_silver_path"]} saved successfully.')


# COMMAND ----------

# exceptAll doesn't validate for duplicates so is faster than subtract that does remove duplicates 
# %timeit df.exceptAll(actual_delta_df)
# %timeit df.subtract(actual_delta_df)

# COMMAND ----------

# from delta.tables import *

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY silver.circuits"))
# display(spark.sql(f"DESCRIBE HISTORY '{path_silver}/{my_parquet_silver_file}'")) # muestra error porque no es una delta table

# COMMAND ----------

# %sql
# ALTER TABLE silver.circuits SET TBLPROPERTIES
# (delta.enableChangeDataFeed=true)

# COMMAND ----------

# cdc_df = spark.readStream.format("delta").option("readChangeData",True).table("silver.circuits")
# display(cdc_df, streamName = "CDC")

# COMMAND ----------

# Load the table
# deltaTable = DeltaTable.forName(spark, "silver.circuits")
# Restore to certain version
# deltaTable.restoreToVersion(6)
