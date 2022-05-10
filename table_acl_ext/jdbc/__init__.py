class _NoDbutilsError(Exception):
    pass


def _get_dbutils():
    try:
        import IPython

        ip_shell = IPython.get_ipython()
        if ip_shell is None:
            raise _NoDbutilsError
        return ip_shell.ns_table["user_global"]["dbutils"]
    except ImportError:
        raise _NoDbutilsError
    except KeyError:
        raise _NoDbutilsError


dbutils = _get_dbutils()


def init():
    dbutils.widgets.text("table_id", "", "Table Id")


def table_id():
    return dbutils.widgets.get("table_id").strip()