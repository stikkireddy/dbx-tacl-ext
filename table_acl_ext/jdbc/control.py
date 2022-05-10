from dataclasses import dataclass
from typing import Dict

from table_acl_ext.jdbc import dbutils


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

    def to_spark_read_builder(self, spark):
        return spark.read \
            .format("com.databricks.spark.sqldw") \
            .option("url", self._jdbc_conn_str) \
            .option("tempDir", self.polybase_azure_storage_loc) \
            .option("forwardSparkAzureStorageCredentials", "true")

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
