# Databricks notebook source
# MAGIC %pip install git+https://github.com/stikkireddy/dbx-tacl-ext.git

# COMMAND ----------

import json
import hashlib

from table_acl_ext.jdbc.control import SynapseTable

# COMMAND ----------

payload = {
    "conn_id": "",
    "synapse_table_info": "",
    "lake_db_name": "",
    "lake_table_name": "",
    "lake_table_loc": "",
    "etl_hour": "",
    "etl_minutes": ""
}
SynapseTable.create_if_not_exists(payload)

