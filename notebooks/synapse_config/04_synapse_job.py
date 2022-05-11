# Databricks notebook source
# MAGIC %pip install git+https://github.com/stikkireddy/dbx-tacl-ext.git

# COMMAND ----------

from table_acl_ext.jdbc import init, get_table_id
from table_acl_ext.jdbc.unload import should_i_unload, unload
from table_acl_ext.jdbc.control import SynapseConnection, SynapseTable

# COMMAND ----------

init()
table_id = get_table_id()

# COMMAND ----------

syn_t = SynapseTable.from_id(table_id)
syn_c = SynapseConnection.from_synapse_table(syn_t)
syn_c, syn_t

# COMMAND ----------

should_unload = should_i_unload(spark, syn_t)
should_unload

# COMMAND ----------

if should_unload is False:
    dbutils.notebook.exit("ALREADY UNLOADED FOR TODAY")
else:
    unload(spark, syn_t, syn_c)