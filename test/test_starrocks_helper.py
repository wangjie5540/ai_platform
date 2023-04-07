import digitforce.aip.common.utils.spark_helper as spark_helper
import findspark

findspark.init()
from pyspark.sql.types import StructType, StructField, LongType, StringType, FloatType

session = spark_helper.SparkClient.get().get_session()

# 根据DDL定义建立schema
schema = StructType([
    StructField("serving_id", LongType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("score", FloatType(), nullable=False)
])

# 创建一个包含测试数据的dataframe
data = [
    (1, "user_4", 90.0),
    (2, "user_5", 80.0),
    (3, "user_3", 85.0),
]

df = session.createDataFrame(data, schema=schema)

# 显示dataframe的内容
df.show()

import digitforce.aip.common.utils.starrocks_helper as starrocks_helper

starrocks_helper.write_score(df, "score_241")
