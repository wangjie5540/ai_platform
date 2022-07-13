# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    结果处理spark版 PS：供应链场景所用
"""
def is_exist_table(spark,check_table):
    """
    判断表是否存在，存在不为空
    :param spark: spark
    :param check_table:检测的表
    :return: 不存在为空，存在不为空
    """
    result=""
    query_sql="""
        SHOW TABLES LIKE "{check_table}"
    """.format(check_table=check_table)
    try:
        datas=spark.sql(query_sql)
        tmp=datas.collect()
        if len(tmp)>0:
            result=tmp[0][1]
    except:
        pass
    return result

def write_to_hive(spark,data,partition,table_name,mode_type):
    """
    写入hive
    :param spark: spark
    :param data: 数据
    :param partition: partition
    :param table_name: 数据名
    :param mode_type: 方式：overwrite、append
    :return:
    """
    if data is not None:
        if is_exist_table(spark,table_name) != "":  # 表示表存在
            data.write.mode(mode_type).insertInto(table_name, True)  # 表示覆盖分区
        else:
            if partition!=[]:
                data.write.partitionBy(partition).mode(mode_type).saveAsTable(table_name)
            else:
                data.write.mode(mode_type).saveAsTable(table_name)


def write_to_csv(location,repartition,encoding,header,file_path,data,mode_type):
    """
    结果写入csv，分为hdfs和local
    :param location: 位置
    :param repartition: repartition
    :param encoding: encoding
    :param header: header
    :param file_path: 保存地址
    :param data: 数据
    :param mode_type:overwrite or append
    :return:
    """
    if location == "hdfs":
        data.write.mode(mode_type).format('csv').repartition(repartition).option("encoding", encoding).option(
            "header", header).save(file_path)
    else:
        data_tmp = data.toPandas()#拉到本地
        data_tmp.to_csv(file_path, index=False, encoding=encoding)