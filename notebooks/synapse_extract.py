# Databricks notebook source
dbutils.widgets.text("table_id", "", "Table Id")
table_id = dbutils.widgets.get("table_id").strip()

# COMMAND ----------

import datetime
from datetime import timedelta


def get_previous_etl(now, now_date, etl_hour, etl_minutes):
    etl_time = (datetime.datetime(*now_date.timetuple()[:4])) + (
                timedelta(hours=int(st.etl_hour)) + timedelta(minutes=int(st.etl_minutes)))
    if etl_time >= now:
        return etl_time - timedelta(days=1)
    else:
        return etl_time


def should_i_unload(st: 'SynapseTable'):
    # todo: catch detailed exceptions
    try:
        now = datetime.datetime.utcnow()
        now_date = now.date()
        previous_etl = get_previous_etl(now, now_date, st.etl_hour, st.etl_minutes)
        h_df = spark.sql(f"DESCRIBE HISTORY `{st.lake_db_name}`.`{st.lake_table_name}`")
        um = h_df.filter(h_df.userMetadata.isNotNull()).orderBy(h_df.version.desc()).select("userMetadata").limit(
            1).collect()[0].userMetadata
        previous_unload = datetime.datetime.strptime(um, "%Y-%m-%d %H:%M:%S")
        print(now, previous_unload, previous_etl)
        return now > previous_etl > previous_unload
    except Exception:
        return True


# COMMAND ----------

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class SynapseConnection:
    conn_id: str
    jdbc_url: str
    jdbc_options: Dict[str, str]
    polybase_azure_storage_loc: str
    password_scope: str
    password_key: str
    storage_scope: str
    storage_key: str

    @property
    def _password(self):
        return dbutils.secrets.get(self.password_scope, self.password_key)

    @property
    def _storage_key(self):
        return dbutils.secrets.get(self.storage_scope, self.storage_key)

    @property
    def _jdbc_conn_str(self):
        return self.jdbc_url + ";" + ";".join(
            [f"{k}={v}" for k, v in self.jdbc_options.items()]) + f";password={self._password}"

    @classmethod
    def from_row(cls, row):
        return cls(**row.asDict())

    @classmethod
    def from_id(cls, _id, spark, table_name="synapse_config.connections"):
        df = spark.table(table_name)
        table = df.filter(df.conn_id == _id).limit(1).collect()
        return cls.from_row(table[0])

    @classmethod
    def from_synapse_table(cls, st: 'SynapseTable', spark, table_name="synapse_config.connections"):
        df = spark.table(table_name)
        df.display()
        table = df.filter(df.conn_id == st.conn_id).limit(1).collect()
        return cls.from_row(table[0])


@dataclass
class SynapseTable:
    table_id: str
    conn_id: str
    synapse_table_info: str
    lake_db_name: str
    lake_table_name: str
    etl_hour: str
    etl_minutes: str

    @classmethod
    def from_row(cls, row):
        return cls(**row.asDict())

    @classmethod
    def from_id(cls, _id, spark, table_name="synapse_config.tables"):
        df = spark.table(table_name)
        df.display()
        table = df.filter(df.table_id == _id).limit(1).collect()
        return cls.from_row(table[0])


# COMMAND ----------

st = SynapseTable.from_id(table_id, spark)
sc = SynapseConnection.from_synapse_table(st, spark)
sc, st

# COMMAND ----------

unload = should_i_unload(st)

# COMMAND ----------

if unload is False:
    dbutils.notebook.exit("ALREADY UNLOADED FOR TODAY")

# COMMAND ----------

spark.sql("""SET fs.azure.account.key.oneenvstorage.blob.core.windows.net={} """.format(sc._storage_key))

# COMMAND ----------

import datetime

unload_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

synapseUser = (spark.read
               .format("com.databricks.spark.sqldw")
               .option("url", sc._jdbc_conn_str)
               .option("tempDir", sc.polybase_azure_storage_loc)
               .option("forwardSparkAzureStorageCredentials", "true")
               .option("dbTable", st.synapse_table_info)
               .load())

synapseUser.write.mode("overwrite").option("userMetadata", unload_time).format("delta").saveAsTable(
    f"{st.lake_db_name}.{st.lake_table_name}")

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY `{st.lake_db_name}`.`{st.lake_table_name}`").display()

# COMMAND ----------


