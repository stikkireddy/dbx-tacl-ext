import uuid
from dataclasses import dataclass
from typing import Dict

from decouple import config

from table_acl_ext.jdbc import dbutils, spark

_synapse_config_database = config("SYNAPSE_CONFIG_DATABASE",
                                  cast=str,
                                  default="synapse_config")

_synapse_tables_config_table = config("SYNAPSE_TABLES_CONFIG_TABLE",
                                      cast=str,
                                      default=f"{_synapse_config_database}.tables")

_synapse_conns_config_table = config("SYNAPSE_CONNS_CONFIG_TABLE",
                                     cast=str,
                                     default=f"{_synapse_config_database}.connections")


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
    def from_id(cls, _id, table_name=_synapse_conns_config_table):
        df = spark.table(table_name)
        table = df.filter(df.conn_id == _id).limit(1).collect()
        return cls.from_row(table[0])

    def set_spark_storage_session(self):
        spark.sql("""SET fs.azure.account.key.oneenvstorage.blob.core.windows.net={} """.format(self._storage_key))

    def to_spark_read_builder(self):
        return spark.read \
            .format("com.databricks.spark.sqldw") \
            .option("url", self._jdbc_conn_str) \
            .option("tempDir", self.polybase_azure_storage_loc) \
            .option("forwardSparkAzureStorageCredentials", "true")

    @classmethod
    def from_synapse_table(cls, st: 'SynapseTable', table_name=_synapse_conns_config_table):
        df = spark.table(table_name)
        df.display()
        table = df.filter(df.conn_id == st.conn_id).limit(1).collect()
        return cls.from_row(table[0])

    @staticmethod
    def setup_table(location=None):
        # TODO: make this a bit more dynamic
        loc_str = f"LOCATION '{location}'" if location is not None else ""
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_synapse_conns_config_table} (
          conn_id STRING,
          jdbc_url STRING,
          jdbc_options MAP<STRING, STRING>,
          password_scope STRING,
          password_key STRING,
          storage_scope STRING,
          storage_key STRING,
          polybase_azure_storage_loc STRING)
        USING delta
        """ + loc_str)

    def __post_init__(self):
        if self.conn_id is None:
            self.conn_id = str(uuid.uuid4())

    def save(self):
        df = spark.createDataFrame([self.__dict__])
        table = f"{_synapse_conns_config_table}"
        print(f"writing to table: {table}")
        df.write.mode("append").saveAsTable(table)

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
    def from_id(cls, _id, table_name=_synapse_tables_config_table):
        df = spark.table(table_name)
        df.display()
        table = df.filter(df.table_id == _id).limit(1).collect()
        return cls.from_row(table[0])

    @staticmethod
    def setup_table(location=None):
        # TODO: make this a bit more dynamic
        loc_str = f"LOCATION '{location}'" if location is not None else ""
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_synapse_tables_config_table} (
          table_id STRING,
          conn_id STRING,
          synapse_table_info STRING,
          lake_db_name STRING,
          lake_table_name STRING,
          etl_hour STRING,
          etl_minutes STRING)
        USING delta
        """ + loc_str)

    def __post_init__(self):
        if self.table_id is None:
            self.table_id = str(uuid.uuid4())

    def save(self):
        df = spark.createDataFrame([self.__dict__])
        table = f"{_synapse_tables_config_table}"
        print(f"writing to table: {table}")
        df.write.mode("append").saveAsTable(table)