# Databricks notebook source
dbutils.widgets.text(name = "entity", defaultValue = "", label = "Entity")

# COMMAND ----------

entity = dbutils.widgets.get("entity")

# COMMAND ----------

from f1_schemas import *

myent = F1_Schemas()
myent.entities[entity]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create delta table, transform it to silver and save it in HIVE and BLOB

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.types as T
# from pyspark.sql.types import DoubleType, StringType, IntegerType, LongType

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

# Set of transformations to do
field_names = df.schema.fieldNames()
# df = (
#     df
#     .withColumn("duration", col("duration").cast('double'))
#     .withColumn("time", col("time").cast('string'))
# ) 


df.schema.fields[5].dataType = T.DoubleType()
schema_old = df.schema # used for casting correctly types further
df.schema.fields[4].dataType = T.StringType()
schema_new = df.schema # used to convert 

df = spark.read.load(attributes["blob_bronce_path"], format="csv", header="true", sep=',', schema=schema_new)

# Joining pitstops df with races to obtain race_date
df_races = spark.table(f"silver.races").withColumn("date", col("date").cast("string")) # read races table casting date as string
df = df.alias("pits") \
.join(df_races.alias("races"), col("pits.raceId") == col("races.raceId"), "left") \
.selectExpr("pits.*", "races.date AS race_date")

display(df.limit(10))

df = join_date_and_time_cols(df, "race_date", "time") 
display(df.limit(10))
df = df.drop("race_date")

# Apply the schema to the existing DataFrame
for i,field in enumerate(schema_old.fields):
    df.schema.fields[i].dataType = field.dataType
display(df.limit(10))
df = (
    df
    .rdd
    .zipWithIndex()
    .toDF()
    .select(col("_2").alias('pitStopsId'), col("_1.*"))
)

display(df.limit(10))
schema_zip = df.schema
field_names = df.schema.fieldNames()

for i,field in enumerate(schema_zip.fields):
    if isinstance(field.dataType, T.LongType) and field_names[i] != "milliseconds": # i = 7 means if fields is not milliseconds
        df = df.withColumn(field_names[i], col(field_names[i]).cast('int'))
display(df.limit(10))


actual_delta_df = spark.table(f'{attributes["silver_name"]}') # Read the latest version of the Silver Delta table
new_records_df = df.exceptAll(actual_delta_df) # Keep just new records comparing the new df against actual version of delta table
new_records_df.write.format("delta").mode("append").saveAsTable(f'{attributes["silver_name"]}') # Append just new records to delta table
df.write.format("parquet").mode("overwrite").save(f'{attributes["blob_silver_path"]}') # overwrite last version in Blob storage with new version of delta table as standard parquet file
print(f'File {attributes["blob_silver_path"]} saved successfully.')


# COMMAND ----------

display(df.dtypes)

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM silver.{entity} LIMIT 10"))

# COMMAND ----------

# exceptAll doesn't validate for duplicates so is faster than subtract that does remove duplicates 
# %timeit df.exceptAll(actual_delta_df)
# %timeit df.subtract(actual_delta_df)

# COMMAND ----------

# from delta.tables import *

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY silver.{entity}"))
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
