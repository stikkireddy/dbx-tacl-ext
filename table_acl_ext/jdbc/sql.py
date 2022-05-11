from typing import List

from sql_metadata import Parser

from table_acl_ext.jdbc.control import DatabaseTable, SynapseTable


def get_tables(sql_stmt) -> List[DatabaseTable]:
    return [DatabaseTable.from_table_identifier(table) for table in Parser(sql_stmt).tables]


def find_synapse_tables(sql_stmt):
    tables = get_tables(sql_stmt)
    synapse_tables = SynapseTable.list()
    return [SynapseTable.find(synapse_tables, table) for table in tables
            if SynapseTable.find(synapse_tables, table) is not None]


def sql(sql_stmt):
    return find_synapse_tables(sql_stmt)