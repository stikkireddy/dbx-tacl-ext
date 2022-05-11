# Databricks notebook source
# MAGIC %pip install git+https://github.com/stikkireddy/dbx-tacl-ext.git

# COMMAND ----------

import json
import hashlib

from table_acl_ext.jdbc.control import SynapseConnection

# COMMAND ----------

payload = {
    "jdbc_url": "",
    "jdbc_options": {
        "database": "",
        "user": "",
        "encrypt": "true",
        "trustServerCertificate": "false",
        "hostNameInCertificate": "",
        "loginTimeout": ""
     },
    "polybase_azure_storage_loc": "",
    "password_scope": "",
    "password_key": "",
    "storage_scope": "",
    "storage_key": "",
}
SynapseConnection.create_if_not_exists(payload)