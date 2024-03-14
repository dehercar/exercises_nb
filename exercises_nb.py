# Databricks notebook source
display(dbutils.fs.ls('dbfs:/databricks-datasets/'))

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/demo.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Magic commands
# MAGIC %lsmagic

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount container to dfs
# MAGIC  

# COMMAND ----------



# COMMAND ----------

containerName = "contdavid"
storageAccountName = "storagedbdavid"
sas = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-03-15T00:17:39Z&st=2024-03-14T16:17:39Z&spr=https&sig=pwpUIx4xe0T%2By%2BQKdedm4xDwK1ayPxfz%2FwLV3hu%2FkZc%3D"

url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"

# COMMAND ----------

dbutils.fs.unmount("/mnt/contdaviddemo/")

# COMMAND ----------

dbutils.fs.mount(
    source = url,
    mount_point= "/mnt/contdaviddemo",
    extra_configs= {config: sas}
)  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read csv pyspark

# COMMAND ----------

df = spark.read.format("csv").option("delimiter",",").option("header",True).option("inferSchema",True).load("/mnt/contdaviddemo/demo.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/"))

# COMMAND ----------

df.write.mode("overwrite").parquet("dbfs:/mnt/contdaviddemo/demoparquet")

# COMMAND ----------

df.select(df.indice).show()
df.select([col for col in df.columns]).show()

# COMMAND ----------

# DBTITLE 1,Add column
df = df.withColumn("valor_q",df.valor_p * 2)
df.show()

# COMMAND ----------

# DBTITLE 1,Add custom column concatating
from pyspark.sql.functions import concat, lit

df = df.withColumn("customId",concat("indice", lit(" ") ,"valor"))
df.show()

# COMMAND ----------

df = df.withColumnRenamed("customId","myId")
df.show()

# COMMAND ----------

df.createOrReplaceTempView("df_tempview")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   MAX(valor_q) AS valor_q_max
# MAGIC FROM df_tempview

# COMMAND ----------

df_tempview = spark.sql(
    """
SELECT 
  MAX(valor_q) AS valor_q_max
FROM df_tempview
    """
)

# COMMAND ----------

df_tempview.write.mode("overwrite").parquet("dbfs:/mnt/contdaviddemo/demoparquet_tempview")

# COMMAND ----------

df.show()

# COMMAND ----------

df.filter(df.valor_q > 1).filter(df.indice > 10).show()

# COMMAND ----------

# DBTITLE 1,Call child notebook
# MAGIC %run ./child_nb

# COMMAND ----------

greet("eliud")

# COMMAND ----------

display(name)

# COMMAND ----------

from ydata_profiling import ProfileReport

profile = df.toPandas().profile_report(title = "myProfile", interactions = None)
displayHTML(profile.html)

# COMMAND ----------

from delta.tables import *

df.write.format("delta").saveAsTable("default.my_delta_table")

# COMMAND ----------

df_delta = DeltaTable.forName(spark,"my_delta_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM my_delta_table

# COMMAND ----------

df.write.insertInto("my_delta_table",overwrite=False)

# COMMAND ----------

display(df_delta.history())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM my_delta_table
