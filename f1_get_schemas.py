# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount F1 input files to DBFS
# MAGIC

# COMMAND ----------

# MAGIC %lsmagic

# COMMAND ----------

containersList = ['bronce','tableschemas']

# COMMAND ----------

for containerName in containersList:
    mount_point = f'/mnt/{containerName}'
    is_mounted_bool = any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts())
    if not is_mounted_bool:
        storageAccountName = "storageaccdavid"
        sas = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-04-06T05:29:32Z&st=2024-04-02T21:29:32Z&spr=https&sig=i18J0JP3szVR8UT2zTSG8g%2BPVPW4CnJsW0lPFkzNEJg%3D" #last until Apr 12th

        url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
        config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"
        dbutils.fs.mount(
            source = url,
            mount_point= mount_point,
            extra_configs= {config: sas}
        ) 


# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

path_bronce = '/mnt/bronce'
path_tableschemas = '/mnt/tableschemas'

# COMMAND ----------

display(dbutils.fs.ls(path_tableschemas))

# COMMAND ----------

df_array = list()
for i, file in enumerate(dbutils.fs.ls(path_bronce)):
    filename_noext = file.name.split('.')[0] 
    print(f"Processing {file.name}...")
    df = spark.read.format("csv").option("delimiter",",").option("header",True).option("inferSchema",True).load(f'{path_bronce}/{file.name}')
    df.createOrReplaceTempView(filename_noext)
    spark.sql(f"DESCRIBE TABLE EXTENDED {filename_noext}").write.csv(f'{filename_noext}.csv')
    print("--------------------------------------------------------------------------")
    break
