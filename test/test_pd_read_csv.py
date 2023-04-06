import digitforce.aip.common.utils.id_helper as id_helper
import pandas as pd
import requests
from io import StringIO

# 添加自定义的 HTTP 头
headers = {

}

columns = 'a,b,c'
column_list = columns.strip().split(',')
df = pd.read_csv("http://172.22.20.45:8658/api/labelx/open/pack/1185/download?targetEntityId=93", names=column_list)
print(df.head())
