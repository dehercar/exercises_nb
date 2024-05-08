# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount F1 input files to DBFS
# MAGIC

# COMMAND ----------

# MAGIC %lsmagic

# COMMAND ----------

containersList = ['input-f1','profiling','bronce', 'silver','gold']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount Blob Containers

# COMMAND ----------

# mount if not exists
for containerName in containersList:
    mount_point = f'/mnt/{containerName}'
    is_mounted_bool = any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts())
    if not is_mounted_bool:
        storageAccountName = "storageaccdavid"
        access_key = dbutils.secrets.get("mydbkv-scope", "storageaccdavid-key")
        

        url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
        config = "fs.azure.account.key." + storageAccountName + ".blob.core.windows.net"
        dbutils.fs.mount(
            source = url,
            mount_point= mount_point,
            extra_configs= {config: access_key}
        ) 


# COMMAND ----------

# hard refresh existing
for containerName in containersList:
    mount_point = f'/mnt/{containerName}'
    is_mounted_bool = any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts())
    if is_mounted_bool:
        dbutils.fs.unmount(mount_point)
        storageAccountName = "storageaccdavid"
        access_key = dbutils.secrets.get("mydbkv-scope", "storageaccdavid-key")
        # sas = "<sas_token>"


        url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
        config = "fs.azure.account.key." + storageAccountName + ".blob.core.windows.net"
        # config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"
        
        dbutils.fs.mount(
            source = url,
            mount_point= mount_point,
            extra_configs= {config: access_key}
            # extra_configs= {config: sas}
        )
    break


# COMMAND ----------

dbutils.fs.mounts()
