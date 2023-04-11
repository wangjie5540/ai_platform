import digitforce.aip.common.utils.spark_helper as spark_helper
import findspark

findspark.init()
from pyspark.sql.types import StructType, StructField, LongType, StringType, FloatType


def do_write():
    session = spark_helper.SparkClient.get().get_session()

    # 数据
    data = [
        (1645666375508107265, 1008, 0.6165969),
        (1645666375508107265, 1008, 0.6165969),
        (1645666375508107265, 1008, 0.6165969),
        (1645666375508107265, 1008, 0.6165969),
        (1645666375508107265, 1008, 0.6165969),
        (1645666375508107265, 1008, 0.6165969),
        (1645666375508107265, 1008, 0.6165969),
        (1645666375508107265, 1007, 0.5454975),
        (1645666375508107265, 1005, 0.50651985),
        (1645666375508107265, 1005, 0.50651985),
    ]

    # 定义schema
    schema = StructType([
        StructField("instance_id", LongType(), True),
        StructField("user_id", StringType(), True),
        StructField("score", FloatType(), True)
    ])

    df = session.createDataFrame(data, schema=schema)
    print(df.schema)

    # 显示dataframe的内容
    df.show()

    import digitforce.aip.common.utils.starrocks_helper as starrocks_helper

    starrocks_helper.write_score(df, "score_251")
