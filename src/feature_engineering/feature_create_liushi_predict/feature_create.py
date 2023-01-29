#!/usr/bin/env python3
# encoding: utf-8
import datetime
from digitforce.aip.common.utils.spark_helper import spark_client
from digitforce.aip.common.utils.hdfs_helper import hdfs_client
import utils

DATE_FORMAT = "%Y%m%d"
today = datetime.datetime.today().strftime(DATE_FORMAT)

def feature_create(predict_samples_table_name,
                   active_before_days, active_after_days,
                   feature_days=30):
    window_test_days = 5
    window_train_days = 30
    now = datetime.datetime.now()
    end_date = now - datetime.timedelta(days=active_after_days + 2)
    mid_date = end_date - datetime.timedelta(days=window_test_days)
    start_date = mid_date - datetime.timedelta(days=window_train_days)
    end_date = end_date.strftime(DATE_FORMAT)
    mid_date = mid_date.strftime(DATE_FORMAT)
    start_date = start_date.strftime(DATE_FORMAT)

    # 活跃度数据起始日期：基于start_date，过去n天，即 start_date - n
    active_start_date = (datetime.datetime.strptime(start_date, '%Y%m%d') - datetime.timedelta(
        days=active_before_days)).strftime("%Y%m%d")
    # 活跃度数据结束日期：基于end_date，未来m天，即，end_date + m
    active_end_date = (
            datetime.datetime.strptime(end_date, '%Y%m%d') + datetime.timedelta(days=active_after_days)).strftime(
        "%Y%m%d")
    # 特征数据最早日期：基于start_date，使用过去k天的数据，即，start_date - k
    feature_date = (datetime.datetime.strptime(start_date, '%Y%m%d') - datetime.timedelta(days=feature_days)).strftime(
        "%Y%m%d")

    # 客户号，年龄，性别，城市，省份，教育程度
    table_user = spark_client.get_session().sql(
        "select cust_code, age, gender, city_name, province, educational_degree from algorithm.sample_jcbq where replace(dt,'-','') = '{}'".format(
            end_date))
    # 客户号，日期，客户是否登录
    table_app = spark_client.get_session().sql(
        "select cust_code, replace(dt,'-','') as dt, is_login from algorithm.sample_llxw where replace(dt,'-','') between '{}' and '{}'".format(
            active_start_date, active_end_date))
    # 客户号，日期，资金转出金额，资金转入金额，资金转出笔数，资金转入笔数
    table_zj = spark_client.get_session().sql(
        "select cust_code, replace(dt,'-','') as dt, zc_money, zr_money, zc_cnt, zr_cnt from algorithm.sample_zjls where replace(dt,'-','') between '{}' and '{}'".format(
            feature_date, end_date))
    # 客户号，日期，交易笔数，交易金额，股票笔数，股票金额，基金笔数，基金金额
    table_jy = spark_client.get_session().sql(
        "select cust_code, replace(dt,'-','') as dt, jy_num, jy_rmb, jygp_num, jygp_rmb, jyjj_num, jyjj_rmb from algorithm.sample_sgsh where replace(dt,'-','') between '{}' and '{}'".format(
            feature_date, end_date))
    # 客户号，日期，总资产，总负债，基金资产，股票资产，资金余额，产品资产
    table_zc = spark_client.get_session().sql(
        "select cust_code, replace(dt,'-','') as dt, ast_total, ast_fz, ast_jj, ast_gp, ast_zj, ast_cp from algorithm.sample_zcsj where replace(dt,'-','') between '{}' and '{}'".format(
            feature_date, end_date))

    # 2. 特征预处理

    # 2.1 客户号 --> [(交易日，交易笔数，交易额，股票交易笔数，股票交易额，基金交易笔数，基金交易额),(),()...]
    jy_feature = table_jy.rdd.filter(lambda x: x[2] and x[2] > 0). \
        map(lambda x: (x[0], [(int(x[1]), x[2], x[3], x[4], x[5], x[6], x[7])])). \
        reduceByKey(lambda a, b: a + b). \
        map(lambda x: (x[0], sorted(x[1], key=lambda y: int(y[0]), reverse=True)))  # 按交易日降序排列

    # 2.2 客户号 --> [(资金变动日期，资金转出金额，资金转入金额，资金转出笔数，资金转入笔数),(),()...]
    zj_feature = table_zj.rdd.filter(lambda x: x[4] and (x[4] > 0 or x[5] > 0)). \
        map(lambda x: (x[0], [[int(x[1]), x[2], x[3], x[4], x[5]]])). \
        reduceByKey(lambda a, b: a + b). \
        map(lambda x: (x[0], sorted(x[1], key=lambda y: int(y[0]), reverse=True)))  # 按日期降序排列

    # 2.3 客户号 --> [活跃日期1, 活跃日期2...]
    act_feature = table_app.rdd.filter(lambda x: x[2] and x[2] > 0). \
        map(lambda x: (x[0], set([int(x[1])]))). \
        reduceByKey(lambda a, b: a | b). \
        map(lambda x: (x[0], sorted(list(x[1]))))  # 按活跃日期升序排列

    # 2.4 (客户号, 日期) --> (总资产, 总负债，基金资产，股票资产，资金余额，产品资产)
    zc_feature = table_zc.rdd.filter(lambda x: x[2]). \
        map(lambda x: ((x[0], x[1]), (x[2], x[3], x[4], x[5], x[6], x[7])))

    # 2.5 客户号 --> (年龄，性别，城市，省份，教育程度)
    user_feature = table_user.rdd.filter(lambda x: x[1]). \
        map(lambda x: (x[0], (x[1], x[2], x[3], x[4], x[5])))

    # 3. 特征拼接

    # 读取样本数据：客户号 --> (日期，label)
    custom_label = spark_client.get_session(). \
        sql("select custom_id, '{}' as date, '0' as label from {}".format(end_date, predict_samples_table_name)). \
        rdd.map(lambda x: (x[0], (x[1], x[2])))

    # 3.1 客户号 --> ((日期，lable)，(最近一次交易距今天数，最近一次交易额，最近3/7/15/30天交易))
    merge_feature1 = custom_label.leftOuterJoin(jy_feature). \
        map(lambda x: (x[0], (x[1][0], utils.get_jy_feature(int(x[1][0][0]), x[1][1]))))

    # 3.2  最近一次资金变动距今天数，最近一次资金变动金额，最近3/7/15/30天的资金变动
    merge_feature2 = merge_feature1.leftOuterJoin(zj_feature). \
        map(lambda x: (x[0], (x[1][0][0][0], x[1][0][0][1], x[1][0][1][0], x[1][0][1][1], x[1][0][1][2],
                              utils.get_zj_feature(int(x[1][0][0][0]), x[1][1])))). \
        map(lambda x: (x[0], (x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5][0], x[1][5][1], x[1][5][2])))

    # 3.3 最近3/7/15/30天的登录天数
    merge_feature3 = merge_feature2.leftOuterJoin(act_feature). \
        map(lambda x: (x[0], (
        x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3], x[1][0][4], x[1][0][5], x[1][0][6], x[1][0][7],
        utils.get_act_feature(int(x[1][0][0]), x[1][1]))))

    # 3.4 客户基本信息(年龄，性别，城市，省份，教育程度)
    merge_feature4 = merge_feature3.join(user_feature). \
        map(lambda x: ((x[0], x[1][0][0]), (
        x[1][0][1], x[1][0][2], x[1][0][3], x[1][0][4], x[1][0][5], x[1][0][6], x[1][0][7], x[1][0][8], x[1][1])))

    # 3.5 当天总资产, 总负债，基金资产，股票资产，资金余额，产品资产
    merge_feature5 = merge_feature4.leftOuterJoin(zc_feature). \
        map(lambda x: x if x[1][1] else (x[0], (x[1][0], (0.0, 0.0, 0.0, 0.0, 0.0, 0.0))))

    # 3.6 格式整理 共59个特征
    merge_feature6 = merge_feature5.map(lambda x: ((x[0][0], x[0][1], x[1][0][0]), \
                                                   [x[1][0][1], x[1][0][2]] + utils.format_list(x[1][0][3]) + \
                                                   [x[1][0][4], x[1][0][5]] + utils.format_list(x[1][0][6]) + \
                                                   x[1][0][7] + list(x[1][0][8]) + list(x[1][1])))

    # 性别
    dict_sex = utils.genDict(merge_feature6.map(lambda x: x[1][49]))
    # dict_sex = genDict(spark.sparkContext.parallelize(["男","女"]))
    write_hdfs_dict(dict_sex, "sex", hdfs_client)

    # 城市
    dict_city = utils.genDict(merge_feature6.map(lambda x: x[1][50]))
    write_hdfs_dict(dict_city, "city", hdfs_client)

    # 省份
    dict_province = utils.genDict(merge_feature6.map(lambda x: x[1][51]))
    write_hdfs_dict(dict_province, "province", hdfs_client)

    # 教育程度
    dict_edu = utils.genDict(merge_feature6.map(lambda x: x[1][52]))
    write_hdfs_dict(dict_edu, "edu", hdfs_client)

    # 3.7 将枚举型转为数值型
    merge_feature7 = merge_feature6.map(lambda x: (x[0], x[1][:49] + [dict_sex.get(x[1][49]), dict_city.get(x[1][50]),
                                                                      dict_province.get(x[1][51]),
                                                                      dict_edu.get(x[1][52])] + x[1][53:]))

    feature_cols = ["custom_id", "label", "last_jy_days", "last_jy_money", "3_jy_cnt", "3_jy_money", "3_jy_gp_cnt",
                    "3_jy_gp_money", "3_jy_jj_cnt", "3_jy_jj_money", "7_jy_cnt", "7_jy_money", "7_jy_gp_cnt",
                    "7_jy_gp_money", "7_jy_jj_cnt", "7_jy_jj_money", "15_jy_cnt", "15_jy_money", "15_jy_gp_cnt",
                    "15_jy_gp_money", "15_jy_jj_cnt", "15_jy_jj_money", "30_jy_cnt", "30_jy_money", "30_jy_gp_cnt",
                    "30_jy_gp_money", "30_jy_jj_cnt", "30_jy_jj_money", "last_zj_days", "last_zj_money",
                    "3_zj_zc_money", "3_zj_zr_money", "3_zj_zc_cnt", "3_zj_zr_cnt", "7_zj_zc_money", "7_zj_zr_money",
                    "7_zj_zc_cnt", "7_zj_zr_cnt", "15_zj_zc_money", "15_zj_zr_money", "15_zj_zc_cnt", "15_zj_zr_cnt",
                    "30_zj_zc_money", "30_zj_zr_money", "30_zj_zc_cnt", "30_zj_zr_cnt", "3_login_cnt", "7_login_cnt",
                    "15_login_cnt", "30_login_cnt", "age", "sex", "city", "province", "edu", "now_zc", "now_fuzhai",
                    "now_zc_jj", "now_zc_gp", "now_zj", "now_zc_cp", "dt"]

    # dt(今天,分区), lable, 最近一次...
    data_predict = merge_feature7.map(lambda x: [x[0][0]] + [x[0][2]] + x[1] + [today])
    print(f"data_predict : {data_predict.count()}")
    data_predict_df = spark_client.get_session().createDataFrame(data_predict, feature_cols)

    predict_table_name = "algorithm.aip_zq_liushi_custom_feature_predict"
    write_hive(data_predict_df, predict_table_name, "dt")

    return predict_table_name




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




def write_hive(inp_df, table_name, partition_col):
    check_table = spark_client.get_session()._jsparkSession.catalog().tableExists(table_name)

    if check_table:  # 如果存在该表
        print("table:{} exist......".format(table_name))
        inp_df.write.format("orc").mode("overwrite").insertInto(table_name)

    else:  # 如果不存在
        print("table:{} not exist......".format(table_name))
        inp_df.write.format("orc").mode("overwrite").partitionBy(partition_col).saveAsTable(table_name)

