from forecast.common.mysql import *
def site_processing(x, df, col_sitelevel, col_shops, col_siteInclude, col_site):
    x = eval(x)
    if x[col_site] == 'shop':
        shops = x[col_siteInclude]
        if shops is None:
            shops = df[col_shops].unique().tolist()
    else:
        city = x[col_siteInclude]
        if city is None:
            shops = df[col_shops].unique().tolist()
        else:
            shops = df[df[col_sitelevel].isin(city)][col_shops].unique().tolist()
    if shops == None:
        print(x)
    return shops


def pred_granularity(x, col_site, col_item, col_time):
    x = eval(x)
    return [x[col_site], x[col_item], x[col_time]]


def list_processing(x):
    re = []
    for a in x:
        for b in a:
            if b is not None:
                re.append(b)
    return list(set(re))
def analysis_task_shop(spark, param):
    """
    新任务解析按门店插表
    """
    origin_task_table = param['origin_task_table']
    origin_site_table = param['origin_site_table']
    shop_status_table = param['shop_status_table']
    col_param = param['col_param']
    col_sitelevel = param['col_sitelevel']
    col_shops = param['col_shops']
    col_list = param['col_list']
    col_site = param['col_site']
    col_item = param['col_item']
    col_time = param['col_time']
    col_siteInclude = param['col_siteInclude']
    col_jobid = param['col_jobid']

    query_task_sql = """select * from {}""".format(origin_task_table)
    query_site_sql = """select * from {}""".format(origin_site_table)
    query_shop_status = """select * from {}""".format(shop_status_table)
    df_task = get_data_from_mysql(query_task_sql)
    df_task[col_param] = df_task[col_param].apply(lambda x: x.replace('null', 'None'))
    df_site = get_data_from_mysql(query_site_sql)
    df_task['shop_id'] = df_task[col_param].apply(
        lambda x: site_processing(x, df_site, col_sitelevel, col_shops, col_siteInclude, col_site))
    df_task['pred_granularity'] = df_task[col_param].apply(
        lambda x: pred_granularity(x, col_site, col_item, col_time)).astype(str)
    # 相同预测粒度的做个合并
    df_job = df_task.groupby('pred_granularity').agg({col_jobid: np.max}).reset_index().rename(columns={col_jobid:'task_id'})
    df_shops = df_task.groupby('pred_granularity').shop_id.apply(list).reset_index()
    df_shops['shop_id'] = df_shops['shop_id'].apply(lambda x: list_processing(x))
    newvalues = np.dstack((np.repeat(df_shops.pred_granularity.values, list(map(len, df_shops.shop_id.values))),
                           np.concatenate(df_shops.shop_id.values)))
    df_shops = pd.DataFrame(data=newvalues[0], columns=df_shops.columns)
    res_df = pd.merge(df_shops, df_job, how='inner', on='pred_granularity')
    df_shop_status = get_data_from_mysql(query_shop_status)
    print(res_df.head(2))
    res_df = pd.merge(res_df, df_shop_status, how='left', on=['task_id', 'shop_id','pred_granularity'])
    res_df = res_df.fillna(1)
    columns = get_table_columns(shop_status_table)
    print("columns",columns)
    print(res_df.dtypes)
    conn='mysql+pymysql://digitforce_arch:arch123456@172.21.32.86:3306/ibs_replenish?charset=utf8'

    res_df[columns].to_sql(shop_status_table, conn, if_exists="replace")
    return 'SUCCESS'


def read_task_list():
    """读取任务列表"""
    pass


def pred_by_new_model():
    """
    新生成的预测模型但没有进行预测的进行预测
    """