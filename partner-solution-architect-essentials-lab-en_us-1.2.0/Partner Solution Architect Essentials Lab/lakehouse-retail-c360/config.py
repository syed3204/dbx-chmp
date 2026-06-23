# Databricks notebook source
# MAGIC %md
# MAGIC ## Configuration file
# MAGIC
# MAGIC Catalog and schema are set dynamically per user.

# COMMAND ----------

catalog = "dbacademy"

# Derive per-user schema from the current username
_username = spark.sql("SELECT current_user()").collect()[0][0]
import re
schema = dbName = db = re.sub(r'[^a-zA-Z0-9_]', '_', _username.split("@")[0])

volume_name = "c360"
