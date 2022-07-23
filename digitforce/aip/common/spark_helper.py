from digitforce.aip.common.data_helper import tuple_self
import pyspark.sql.functions as psf


def is_exist_table(spark, check_table):
    """
    判断表是否存在
    :param spark:
    :param check_table: 要检查的表
    :return:
    """
    result = False
    try:
        if spark.table("{0}".format(check_table)):
            result = True
    except:
        pass
    return result


def show_columns(spark, check_table):
    columns = spark.sql("show columns in {0}".format(check_table)).toPandas()['col_name'].tolist()
    return columns


def read_table(spark, table_name, sdt='Y', dt="dt", partition_name='shop_id', partition_list=[]):
    """dt:分区字段
       sdt:时间戳字段
    """
    filter_str = ""
    if len(partition_list) > 0:
        filter_str = " where {0} in {1}".format(partition_name, tuple_self(partition_list))
    sparkdf = spark.sql("""select * from {0} {1} """.format(table_name, filter_str))
    if sdt == 'N':
        sparkdf = sparkdf.withColumn("sdt", psf.unix_timestamp(psf.to_timestamp(psf.col(dt), 'yyyyMMdd'),
                                                               "format='yyyy-MM-dd"))
    return sparkdf


def save_table(spark, sparkdf, table_name, save_mode='overwrite', partition=["shop_id", "dt"]):
    if is_exist_table(spark, table_name):
        columns = show_columns(spark, table_name)
        print(columns, table_name)
        sparkdf.repartition(1).select(columns).write.mode("overwrite").insertInto(table_name, True)
    else:
        print("save table name", table_name)
        sparkdf.write.mode(save_mode).partitionBy(partition).saveAsTable(table_name)


def read_origin_table(spark, table_name, query_sql, col_name='', col_value=[]):
    if len(col_value) > 0:
        filter_str = " where {0} in {1}".format(col_name, tuple_self(col_value))
    else:
        filter_str = " where 1=1 "
    sparkdf = spark.sql(query_sql.format(table_name, filter_str))
    return sparkdf

# def write_to_csv(location, repartition, encoding, header, file_path, data, mode_type):
#     """
#     结果写入csv，分为hdfs和local
#     :param location: 位置
#     :param repartition: repartition
#     :param encoding: encoding
#     :param header: header
#     :param file_path: 保存地址
#     :param data: 数据
#     :param mode_type:overwrite or append
#     :return:
#     """
#     if location == "hdfs":
#         data.write.mode(mode_type).format('csv').repartition(repartition).option("encoding", encoding).option(
#             "header", header).save(file_path)
#     else:
#         data_tmp = data.toPandas()  # 拉到本地
#         data_tmp.to_csv(file_path, index=False, encoding=encoding)
