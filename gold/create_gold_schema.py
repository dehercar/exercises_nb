# Databricks notebook source
# MAGIC %md
# MAGIC ### Create silver schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use silver schema

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA gold
