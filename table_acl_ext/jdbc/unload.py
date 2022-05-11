import datetime
import traceback
from datetime import timedelta

from table_acl_ext.jdbc.control import SynapseTable, SynapseConnection


def get_previous_etl(now, now_date, etl_hour, etl_minutes):
    etl_time = (datetime.datetime(*now_date.timetuple()[:4])) + (
                timedelta(hours=int(etl_hour)) + timedelta(minutes=int(etl_minutes)))
    if etl_time >= now:
        return etl_time - timedelta(days=1)
    else:
        return etl_time


def should_i_unload(spark, st: 'SynapseTable'):
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
    except Exception as e:
        traceback.print_exc()
        return True


def unload(spark, st: 'SynapseTable', sc: 'SynapseConnection'):
    sc.set_spark_storage_session()
    unload_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    synapse_df = sc.to_spark_read_builder().option("dbTable", st.synapse_table_info).load()
    synapse_df.write.mode("overwrite").option("userMetadata", unload_time).format("delta").saveAsTable(
        f"{st.lake_db_name}.{st.lake_table_name}")
    spark.sql(f"DESCRIBE HISTORY `{st.lake_db_name}`.`{st.lake_table_name}`").display()