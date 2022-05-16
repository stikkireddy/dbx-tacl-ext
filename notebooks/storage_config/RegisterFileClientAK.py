# Databricks notebook source
# MAGIC %pip install git+https://github.com/stikkireddy/dbx-tacl-ext.git

# COMMAND ----------

from table_acl_ext.storage.adls import ADLSAccessKeyCredentialsManager

# COMMAND ----------

manager = ADLSAccessKeyCredentialsManager()

# COMMAND ----------

manager.init()

# COMMAND ----------

dbutils.notebook.exit()

# COMMAND ----------

manager.register()

# COMMAND ----------

dbutils.notebook.exit()

# COMMAND ----------

from table_acl_ext.storage import list_storages, get_client
list_storages()
client = get_client("<storage key>")
client.ls("/")
