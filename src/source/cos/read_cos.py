import digitforce.aip.common.utils.spark_helper as spark_helper
import pandas as pd
import digitforce.aip.common.utils.id_helper as id_helper


def read_to_table(url: str, columns: str):
    df = pd.read_csv(url, names=columns.split(','))
    df = spark_helper.spark_session.createDataFrame(df)
    table_name = f'aip.cos_{id_helper.gen_uniq_id()}'
    df.write.format("hive").mode("overwrite").saveAsTable(table_name)
    return table_name, columns
