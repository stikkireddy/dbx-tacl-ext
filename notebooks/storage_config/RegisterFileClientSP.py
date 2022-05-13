# Databricks notebook source
# MAGIC %pip install git+https://github.com/stikkireddy/dbx-tacl-ext.git

# COMMAND ----------

from table_acl_ext.storage.adls import ADLSSPCredentialsManager

# COMMAND ----------

manager = ADLSSPCredentialsManager()

# COMMAND ----------

manager.init()

# COMMAND ----------

manager.register()
