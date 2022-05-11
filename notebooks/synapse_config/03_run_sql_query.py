# Databricks notebook source
# MAGIC %pip install git+https://github.com/stikkireddy/dbx-tacl-ext.git

# COMMAND ----------

from table_acl_ext.jdbc.sql import sql
import os
os.environ["SYNAPSE_UNLOAD_JOB_ID"] = ""

display(sql("""
    SELECT * FROM synapse_tmp.users2
"""))

# COMMAND ----------

