import digitforce.aip.common.utils.spark_helper as spark_helper
import pandas as pd

df = pd.read_csv("https://algorithm-1308011215.cos.ap-beijing.myqcloud.com/1675332242109-seeds.csv",
                 names='a'.split(','))
df = spark_helper.SparkClient.get().get_session().createDataFrame(df)
df.show()

df.write.format("com.digitforce.bdp.StarrocksSource") \
    .option("database-name", "aip") \
    .option("table-name", "wtg_test") \
    .option("jdbc-url", "jdbc:mysql://dev-common-starrocks-n1.digitforce.com:9030") \
    .option("load-url", "172.24.20.42:8030") \
    .option("username", "root") \
    .option("password", "") \
    .save()
