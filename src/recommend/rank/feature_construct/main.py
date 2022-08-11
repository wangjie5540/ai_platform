import json

from digitforce.aip.common.hive_helper import df_hive_helper


def construct_features(config_file, user_features_file, item_features_file):

    sql_user = '''select sex,province, city from labelx.push_user'''
    sql_sku = '''select cate, brand from labelx.push_goods'''
    user_df = df_hive_helper.query_to_df(sql_user)
    sku_df = df_hive_helper.query_to_df(sql_sku)

    sex_dict = get_dict(user_df['sex'].unique())
    province_dict = get_dict(user_df['province'].unique())
    city_dict = get_dict(user_df['city'].unique())
    cate_dict = get_dict(sku_df['cate'].unique())
    brand_dict = get_dict(sku_df['brand'].unique())

    config_dict = {'sex': sex_dict, 'province': province_dict, 'city': city_dict,
                   'cate': cate_dict, 'brand': brand_dict}
    with open(config_file, 'w') as f0:
        json.dump(config_dict, f0)

    u_sql = '''select user_id, sex, age, province, city, life_stage, consume_level, membership_level from 
                            (select user_id, sex, age, life_stage, consume_level, province, city, membership_level,
                            row_number() over(partition by user_id order by dt desc) rk 
                            from labelx.push_user) u
                            where rk = 1'''
    user_info = df_hive_helper.query_to_df(u_sql)
    user_info['sex'] = user_info['sex'].apply(lambda x: sex_dict.get(x, 0))
    user_info['province'] = user_info['province'].apply(lambda x: province_dict.get(x, 0))
    user_info['city'] = user_info['city'].apply(lambda x: city_dict.get(x, 0))
    user_info.to_csv(user_features_file, index=False)

    g_sql = f'''
            select sku, cate, brand from(
                select sku, cate, brand, row_number() over(partition by sku order by dt desc)rk from labelx.push_goods
                where dt in('2022-03-01', '2022-03-18', '2022-03-31')) goods
                where rk = 1
            '''
    goods_info = df_hive_helper.query_to_df(g_sql)
    goods_info['cate'] = goods_info['cate'].apply(lambda x: cate_dict.get(x, 0))
    goods_info['brand'] = goods_info['brand'].apply(lambda x: brand_dict.get(x, 0))
    goods_info.to_csv(item_features_file, index=False)


def get_dict(lt):
    item_dict = {}
    for i, item in enumerate(lt):
        item_dict[item] = i + 1
    return item_dict
