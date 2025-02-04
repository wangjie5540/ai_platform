# encoding: utf-8
import findspark

findspark.init()

from digitforce.aip.common.utils.spark_helper import SparkClient
from digitforce.aip.common.utils.time_helper import DATE_FORMAT
import datetime

import pyspark.sql.functions as F

today = datetime.datetime.today().strftime(DATE_FORMAT)


def feature_create(
        sample_table_name: str,
        dixiao_before_days: int,
        dixiao_after_days: int,
        feature_days=180,
):
    """产生训练数据集和验证数据集（回测数据集）
    TODO: 预测特征参考liushi重新造

    Args:
        sample_table_name (str): 标签表，在HIVE里
        dixiao_before_days (int): _description_
        dixiao_after_days (int): _description_
        feature_days (int, optional): _description_. Defaults to 180.

    Returns:
        _type_: _description_
    """

    spark_client = SparkClient.get()
    spark = spark_client.get_session()
    user_table = "zq_standard.dm_cust_label_base_attributes_df"
    app_table = "zq_standard.dm_cust_traf_behv_aggregate_df"
    zj_table = "zq_standard.dm_cust_capital_flow_aggregate_df"
    jy_table = "zq_standard.dm_cust_subs_redm_event_aggregate_df"
    zc_table = "zq_standard.dm_cust_ast_redm_event_df"
    user_view = user_table[user_table.find(".") + 1:]
    app_view = app_table[app_table.find(".") + 1:]
    zj_view = zj_table[zj_table.find(".") + 1:]
    jy_view = jy_table[jy_table.find(".") + 1:]
    zc_view = zc_table[zc_table.find(".") + 1:]

    # 1.获取关键时间点
    window_test_days = 3
    window_train_days = 5
    now = datetime.datetime.now()
    dixiao_end_date = now - datetime.timedelta(days=2)  # 低效户结束日期
    end_date = dixiao_end_date - datetime.timedelta(days=dixiao_after_days)  # 低效户结束日期
    mid_date = end_date - datetime.timedelta(days=window_test_days)
    start_date = mid_date - datetime.timedelta(days=window_train_days)
    dixiao_start_date = start_date - datetime.timedelta(
        days=dixiao_before_days
    )  # 低效户开始日期
    feature_date = dixiao_start_date - datetime.timedelta(days=feature_days)  # 特征数据最早日期

    now = now.strftime(DATE_FORMAT)
    dixiao_end_date = dixiao_end_date.strftime(DATE_FORMAT)
    end_date = end_date.strftime(DATE_FORMAT)
    mid_date = mid_date.strftime(DATE_FORMAT)
    start_date = start_date.strftime(DATE_FORMAT)
    dixiao_start_date = dixiao_start_date.strftime(DATE_FORMAT)
    feature_date = feature_date.strftime(DATE_FORMAT)
    # 2. 特征预处理
    spark_client.get_starrocks_table_df(user_table).createOrReplaceTempView(user_view)
    spark_client.get_starrocks_table_df(app_table).createOrReplaceTempView(app_view)
    spark_client.get_starrocks_table_df(zj_table).createOrReplaceTempView(zj_view)
    spark_client.get_starrocks_table_df(jy_table).createOrReplaceTempView(jy_view)
    spark_client.get_starrocks_table_df(zc_table).createOrReplaceTempView(zc_view)

    # 客户号，年龄，性别，城市，省份，教育程度(end_date 的基础信息)
    table_user = spark.sql(
        f"""
        select cust_code, age, sex, city_name, province_name, educational_degree 
        from {user_view} where dt = '{end_date}'
        """
    )
    # 客户号，日期，客户是否登录
    table_app = spark.sql(
        f"""
        select cust_code, dt, is_login from {app_view} 
        where dt between '{feature_date}' and '{end_date}'
        """
    )
    # 客户号，日期，资金转出金额，资金转入金额，资金转出笔数，资金转入笔数
    table_zj = spark.sql(
        f"""
        select cust_code, dt, transfer_out_amt, transfer_in_amt, transfer_out_cnt, transfer_in_cnt 
        from {zj_view} 
        where dt between '{feature_date}' and '{end_date}'
        """
    )
    # 客户号，日期，交易笔数，交易金额，股票笔数，股票金额，基金笔数，基金金额, 债券笔数， 债券金额
    table_jy = spark.sql(
        f"""
        select cust_code, dt, total_tran_cnt, total_tran_amt, gp_tran_cnt, gp_tran_amt, jj_tran_cnt, jj_tran_amt, zq_tran_cnt, zq_tran_amt 
        from {jy_view} 
        where dt between '{feature_date}' and '{end_date}'
        """
    )
    # 客户号，日期，总资产，净资产，总负债，非货币型基金资产，股票资产，资金余额，产品资产
    table_zc = spark.sql(
        f"""
        select cust_code, dt, total_ast, net_ast, total_liab, unmoney_fnd_val, stock_ast, cash_bal, total_prd_ast 
        from {zc_view} 
        where dt between '{feature_date}' and '{end_date}'
        """
    )

    # 3. 特征融合
    data = (
        spark.sql(
            f"""
            SELECT cust_code,label,dt
            FROM {sample_table_name}
            WHERE dt>= '{dixiao_start_date}' and dt <= '{end_date}'
            """
        )
        .join(table_user, on=["cust_code"], how="left")
        .join(table_app, on=["cust_code", "dt"], how="left")
        .join(table_zj, on=["cust_code", "dt"], how="left")
        .join(table_jy, on=["cust_code", "dt"], how="left")
        .join(table_zc, on=["cust_code", "dt"], how="left")
    )
    # TODO: 特征加工
    print("dixiao_start_date-----", dixiao_start_date)
    print("end_date-----", end_date)
    print("特征数据规模-----", len(data.toPandas()))
    final_cols = [
        "cust_code",
        "label",
        "age",
        "sex",
        "city_name",
        "province_name",
        "educational_degree",
        "is_login",
        "transfer_out_amt",
        "transfer_in_amt",
        "transfer_out_cnt",
        "transfer_in_cnt",
        "total_tran_cnt",
        "total_tran_amt",
        "gp_tran_cnt",
        "gp_tran_amt",
        "jj_tran_cnt",
        "jj_tran_amt",
        "zq_tran_cnt",
        "zq_tran_amt",
        "total_ast",
        "net_ast",
        "total_liab",
        "unmoney_fnd_val",
        "stock_ast",
        "cash_bal",
        "total_prd_ast",
        "dt",
    ]
    data_train_df = data.filter(F.col("dt") <= mid_date).select(final_cols).distinct()
    data_test_df = data.filter(F.col("dt") > mid_date).select(final_cols).distinct()

    print("训练数据规模-----", len(data_train_df.toPandas()))
    print("测试数据规模-----", len(data_test_df.toPandas()))

    train_table_name = "algorithm.aip_zq_dixiaohu_custom_feature_train_test"
    test_table_name = "algorithm.aip_zq_dixiaohu_custom_feature_test_test"

    if len(data_train_df.toPandas()) == 0 or len(data_test_df.toPandas()) == 0:
        print("数据为空，不写入hive")
        return train_table_name, test_table_name

    train_partition_list = data_train_df.select("dt").distinct().toPandas()["dt"].tolist()
    test_partition_list = data_test_df.select("dt").distinct().toPandas()["dt"].tolist()
    for dt in train_partition_list:
        write_hive(
            spark=spark,
            inp_df=data_train_df,
            table_name=train_table_name,
            partition_col="dt",
            partition_val=dt,
            cols_list=final_cols,
        )  # 按照日期分区写入hive
    for dt in test_partition_list:
        write_hive(
            spark=spark,
            inp_df=data_test_df,
            table_name=test_table_name,
            partition_col="dt",
            partition_val=dt,
            cols_list=final_cols,
        )

    return train_table_name, test_table_name


# 写hdfs，覆盖写！
def write_hdfs_path(local_path, hdfs_path, hdfs_client):
    if hdfs_client.exists(hdfs_path):
        hdfs_client.delete(hdfs_path)
    hdfs_client.copy_from_local(local_path, hdfs_path)


# dict先写本地，再写入hdfs
def write_hdfs_dict(content, file_name, hdfs_client):
    local_path = "dict.{}.{}".format(today, file_name)
    hdfs_path = "/user/ai/aip/zq/liushi/enum_dict/{}/{}".format(today, file_name)

    with open(local_path, "w") as f:
        for key in content:
            f.write("{}\t{}\n".format(key, content[key]))
    write_hdfs_path(local_path, hdfs_path, hdfs_client)


def write_hive(spark, inp_df, table_name, partition_col, partition_val, cols_list):
    check_table = (
        spark._jsparkSession.catalog().tableExists(table_name)
    )

    if check_table:  # 如果存在该表
        print("table:{} exist......".format(table_name))
        (
            inp_df
            .filter(f"{partition_col}='{partition_val}'")
            .drop(partition_col)
            .createOrReplaceTempView("test_temp")
        )  # 创建临时表
        cols_list_copy = cols_list.copy()
        cols_list_copy.remove(partition_col)  # copy后去除分区字段,防止重复删除
        cols_str = str(cols_list_copy).replace("[", "").replace("]", "").replace("'", "")
        spark.sql(
            f"""
            insert overwrite table {table_name} partition({partition_col}='{partition_val}') 
            select {cols_str}
            from test_temp 
            """)

    else:  # 如果不存在
        print("table:{} not exist......".format(table_name))
        inp_df.write.format("orc").mode("overwrite").partitionBy(
            partition_col
        ).saveAsTable(table_name)
