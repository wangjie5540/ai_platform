import digitforce.aip.common.utils.spark_helper as spark_helper
import pandas as pd

df = pd.read_csv("http://algorithm-1308011215.cos.ap-beijing.myqcloud.com/result.csv", names='a,b'.split(','))
spark_helper.spark_session.createDataFrame(df).show()