from table_acl_ext import dbutils

TABLE_ID_WIDGET_PARAM = "table_id"

def init():
    dbutils.widgets.text(TABLE_ID_WIDGET_PARAM, "", "Table Id")


def get_table_id():
    return dbutils.widgets.get(TABLE_ID_WIDGET_PARAM).strip()