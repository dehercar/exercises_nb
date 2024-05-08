# Databricks notebook source
# MAGIC %fs ls 'mnt/profiling'

# COMMAND ----------

path_inputf1 = '/mnt/input-f1'
path_profiling = '/mnt/profiling'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Profiling Files

# COMMAND ----------

def exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            print('The path does not exist')
            return False
        else:
            raise IOError

# COMMAND ----------

from pyspark.sql.types import DateType

# COMMAND ----------

storageAccountName = "storageaccdavid"
for i, file in enumerate(dbutils.fs.ls(path_inputf1)):
    filename_noext = file.name.split('.')[0] 
    my_html_file = f"{i}-{filename_noext}.html"
    if not exists(f'{path_profiling}/{my_html_file}'):
    #if not Path(os.path.join(path_profiling, my_html_file)).is_file():
        print(f"Processing {file.name} to create {path_profiling}/{my_html_file}")
        df = spark.read.format("csv").option("delimiter",",").option("header",True).option("inferSchema",True).load(f'{path_inputf1}/{file.name}')
        df_pandas = df.toPandas()

        # Check if the dob column in the Spark DataFrame has the correct date type
        for column in df_pandas.columns:
            if isinstance(df.schema[column].dataType, DateType):
                df_pandas[column] = pd.to_datetime(df_pandas[column])  # Convert the dob column to Pandas datetime type
        profile = df_pandas.profile_report(title = f'{i}: {file.name}')
        dbutils.fs.put(f'{path_profiling}/{my_html_file}', template('wrapper').render(content=profile.html))
        print(f"{my_html_file} saved successfully.")
        display(df_pandas.info())
        display(df_pandas.head())
        print("--------------------------------------------------------------------------")
    else:
       print(f"{my_html_file} already exists.") 
       print("--------------------------------------------------------------------------")

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
