import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional
import urllib.parse

from decouple import config

from table_acl_ext import dbutils, spark

_synapse_config_database = config("SYNAPSE_CONFIG_DATABASE",
                                  cast=str,
                                  default="synapse_config")

_synapse_tables_config_table = config("SYNAPSE_TABLES_CONFIG_TABLE",
                                      cast=str,
                                      default=f"{_synapse_config_database}.tables")

_synapse_conns_config_table = config("SYNAPSE_CONNS_CONFIG_TABLE",
                                     cast=str,
                                     default=f"{_synapse_config_database}.connections")


def clean_identifier(table_identifier):
    return table_identifier.replace("`", "")


def get_delete_stmt(table_identifier, where_col, where_value):
    return f"DELETE FROM {table_identifier} WHERE {where_col} = '{where_value}'"


@dataclass
class DatabaseTable:
    table: str
    database: str = "default"
    catalog: Optional[str] = None

    @classmethod
    def from_table_identifier(cls, table_identifier):
        cleaned_id = clean_identifier(table_identifier)
        parts = cleaned_id.split(".")
        if len(parts) == 1:
            return cls(table=parts[0])
        if len(parts) == 2:
            return cls(database=parts[0], table=parts[1])
        elif len(parts) == 3:
            return cls(database=parts[1], table=parts[2])
        else:
            print(f"Illegal Identifier: {table_identifier}")
            return None


@dataclass
class SynapseConnection:
    jdbc_url: str
    jdbc_options: Dict[str, str]
    polybase_azure_storage_loc: str
    password_scope: str
    password_key: str
    storage_scope: str
    storage_key: str
    conn_id: str = str(uuid.uuid4())

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
        storage_container = self.polybase_azure_storage_loc.split("@")[1].split(".")[0]
        spark.sql("""SET fs.azure.account.key.{}.blob.core.windows.net={} """.format(storage_container,
                                                                                     self._storage_key))

    def to_spark_read_builder(self):
        print(f"[CONNECTION_STRING]: {self._jdbc_conn_str}")
        print(f"[TEMP STORAGE]: {self.polybase_azure_storage_loc}")
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

    def _save(self):
        df = spark.createDataFrame([self.__dict__])
        table = f"{_synapse_conns_config_table}"
        print(f"writing to table: {table} for connection id: {self.conn_id}")
        print(f"to delete please run: {get_delete_stmt(table, 'conn_id', self.conn_id)}")
        df.write.mode("append").saveAsTable(table)

    @staticmethod
    def list():
        rows = spark.table(_synapse_conns_config_table).collect()
        return [SynapseConnection.from_row(row) for row in rows]

    def md5_checksum(self):
        valid_dict = {k: v for k, v in self.__dict__.items() if k != "conn_id"}
        return hashlib.md5(json.dumps(valid_dict, sort_keys=True).encode('utf-8')).hexdigest()

    def validate(self):
        assert self._password is not None, "password is invalid"
        assert self._storage_key is not None, "storage key is invalid"
        self.set_spark_storage_session()
        self.to_spark_read_builder().option("query", "SELECT 1 as col").load().count()

    @staticmethod
    def create_if_not_exists(data):
        input_chk_sum = hashlib.md5(json.dumps(data, sort_keys=True).encode('utf-8')).hexdigest()
        for conn in SynapseConnection.list():
            if input_chk_sum == conn.md5_checksum():
                raise Exception(f"Connection already exists: {conn}")
        sc = SynapseConnection(**data)
        sc.validate()
        sc._save()


@dataclass
class SynapseTable:
    conn_id: str
    synapse_table_info: str
    lake_db_name: str
    lake_table_name: str
    etl_hour: str
    etl_minutes: str
    table_id: str = str(uuid.uuid4())
    lake_table_loc: str = None

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
          lake_table_loc STRING,
          etl_hour STRING,
          etl_minutes STRING)
        USING delta
        """ + loc_str)

    def _save(self):
        df = spark.createDataFrame([self.__dict__])
        table = f"{_synapse_tables_config_table}"
        print(f"writing to table: {table} for table id: {self.table_id}")
        print(f"to delete please run: {get_delete_stmt(table, 'table_id', self.table_id)}")
        df.write.mode("append").saveAsTable(table)

    # create table if not exists
    @staticmethod
    def create_if_not_exists(data):
        st = SynapseTable(**data)
        for table in SynapseTable.list():
            if st.synapse_table_info == table.synapse_table_info and st.lake_db_name == table.lake_db_name and st.lake_table_name == table.lake_table_name:
                raise Exception(f"Table already exists: {table}")
        sc = SynapseConnection.from_synapse_table(st)
        # count check
        sc.set_spark_storage_session()
        sc.to_spark_read_builder().option("dbTable", st.synapse_table_info).load().count()
        if st.lake_table_loc is None or st.lake_table_loc == "":
            # create table
            sc.to_spark_read_builder().option("dbTable", st.synapse_table_info).load().filter("1==2").write.mode(
                "append").saveAsTable(f"{st.lake_db_name}.{st.lake_table_name}")
        else:
            sc.to_spark_read_builder().option("dbTable", st.synapse_table_info).load().filter("1==2").write.mode(
                "append").saveAsTable(f"{st.lake_db_name}.{st.lake_table_name}", path=st.lake_table_loc)
        st._save()

    @staticmethod
    def list():
        rows = spark.table(_synapse_tables_config_table).collect()
        return [SynapseTable.from_row(row) for row in rows]

    @staticmethod
    def find(synapse_tables: List['SynapseTable'], table_id: Optional[DatabaseTable]) -> Optional['SynapseTable']:
        if table_id is None:
            print("Invalid table identifier. Unable to validate synapse table.")
            return None
        for table in synapse_tables:
            if table.lake_db_name == table_id.database and table.lake_table_name == table_id.table:
                return table
        return None

    def md5_checksum(self):
        valid_dict = {k: v for k, v in self.__dict__.items() if k != "table_id"}
        return hashlib.md5(json.dumps(valid_dict, sort_keys=True).encode('utf-8')).hexdigest()
