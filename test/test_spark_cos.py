import digitforce.aip.common.utils.spark_helper as spark_helper
import pandas as pd

df = pd.read_csv("https://algorithm-1308011215.cos.ap-beijing.myqcloud.com/1675332242109-seeds.csv", names='a'.split(','))
spark_helper.SparkClient.get().get_session().createDataFrame(df).show()

