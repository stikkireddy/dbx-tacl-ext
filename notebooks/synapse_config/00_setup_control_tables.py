# Databricks notebook source
# MAGIC %pip install git+https://github.com/stikkireddy/dbx-tacl-ext.git

# COMMAND ----------

from table_acl_ext.jdbc.control import SynapseConnection, SynapseTable

# COMMAND ----------

SynapseConnection.setup_table()
SynapseTable.setup_table()
