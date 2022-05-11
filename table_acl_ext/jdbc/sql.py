from typing import List

from sql_metadata import Parser

from table_acl_ext.jdbc import TABLE_ID_WIDGET_PARAM, spark
from table_acl_ext.jdbc.control import DatabaseTable, SynapseTable
from table_acl_ext.jdbc.unload import should_i_unload
from table_acl_ext.jobs import par_execute


def get_tables(sql_stmt) -> List[DatabaseTable]:
    unique_table_list = list(set(Parser(sql_stmt).tables))
    return [DatabaseTable.from_table_identifier(table) for table in unique_table_list]


def find_synapse_tables(sql_stmt):
    tables = get_tables(sql_stmt)
    synapse_tables = SynapseTable.list()
    return [SynapseTable.find(synapse_tables, table) for table in tables
            if SynapseTable.find(synapse_tables, table) is not None and
            should_i_unload(spark, SynapseTable.find(synapse_tables, table)) is True]


def sql(sql_stmt):
    table_ids = [{TABLE_ID_WIDGET_PARAM: synapse_table.table_id} for synapse_table in find_synapse_tables(sql_stmt)]
    print(par_execute(table_ids))
    return spark.sql(sql_stmt)
