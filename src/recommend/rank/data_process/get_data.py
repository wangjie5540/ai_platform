import jsonlines
import os
import logging

from digitforce.aip.common.hive_helper import df_hive_helper


def get_data(sql, output_file, config_file, user_features_file, item_features_file):

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
    make_dir(config_file)
    with jsonlines.open(config_file, 'w') as writer:
        writer.write(config_dict)

    u_sql = '''select user_id, sex, age, province, city, life_stage, consume_level, membership_level from 
                                (select user_id, sex, age, life_stage, consume_level, province, city, membership_level,
                                row_number() over(partition by user_id order by dt desc) rk 
                                from labelx.push_user) u
                                where rk = 1'''
    user_info = df_hive_helper.query_to_df(u_sql)
    user_info['sex'] = user_info['sex'].apply(lambda x: sex_dict.get(x, 0))
    user_info['province'] = user_info['province'].apply(lambda x: province_dict.get(x, 0))
    user_info['city'] = user_info['city'].apply(lambda x: city_dict.get(x, 0))
    logging.info(f'user_info shape: {user_info.shape}')
    make_dir(user_features_file)
    with jsonlines.open(user_features_file, 'w') as user_writer:
        for user_dict in user_info.to_dict('records'):
            user_writer.write(user_dict)

    g_sql = f'''
                select sku, cate, brand from(
                    select sku, cate, brand, row_number() over(partition by sku order by dt desc)rk from labelx.push_goods
                    where dt in('2022-03-01', '2022-03-18', '2022-03-31')) goods
                    where rk = 1
                '''
    goods_info = df_hive_helper.query_to_df(g_sql)
    goods_info['cate'] = goods_info['cate'].apply(lambda x: cate_dict.get(x, 0))
    goods_info['brand'] = goods_info['brand'].apply(lambda x: brand_dict.get(x, 0))
    logging.info(f'goods_info shape {goods_info.shape}')
    make_dir(item_features_file)
    with jsonlines.open(item_features_file, 'w') as goods_writer:
        for goods_dict in goods_info.to_dict('records'):
            goods_writer.write(goods_dict)

    logging.info("read data from hive")
    df = df_hive_helper.query_to_df(sql)
    df['sex'] = df['sex'].apply(lambda x: sex_dict.get(x, 0))
    df['province'] = df['province'].apply(lambda x: province_dict.get(x, 0))
    df['city'] = df['city'].apply(lambda x: city_dict.get(x, 0))
    df['cate'] = df['cate'].apply(lambda x: cate_dict.get(x, 0))
    df['brand'] = df['brand'].apply(lambda x: brand_dict.get(x, 0))

    columns = ['sex', 'age', 'province', 'city', 'life_stage', 'consume_level',
               'membership_level', 'cate', 'brand', 'label']
    df = df[columns]
    logging.info("data processing end")

    make_dir(output_file)
    logging.info(f"write data to {output_file}")
    logging.info(f'sample shape: {df.shape}')
    df.to_csv(output_file, index=False)


def get_dict(lt):
    item_dict = {}
    for i, item in enumerate(lt):
        item_dict[item] = i + 1
    return item_dict


def make_dir(file):
    dir_name = os.path.dirname(file)
    logging.info(f"dir_name: {dir_name}")
    if dir_name and not os.path.exists(dir_name):
        logging.info(f"mkdir -p {dir_name}")
        os.system(f"mkdir -p {dir_name}")


if __name__ == '__main__':
    sql_test = f'''
select a.user_id,sex, age, life_stage, consume_level, province, city, membership_level,  b.sku, cate, brand, label from 
(select * from 
(select user_id, sex, age, life_stage, consume_level, province, city, membership_level,
row_number() over(partition by user_id order by dt desc) rk from labelx.push_user) t0
where rk = 1
) a
inner join 
(select user_id, sku, max(label) as label from(
select user_id, sku, case when event_code='CLICK' then 1 else 0 end as label from labelx.push_traffic_behavior 
where  event_code in ('CLICK', 'EXPOSURE'))bh group by user_id, sku ) b 
on a.user_id = b.user_id 
inner join 
(select * from(
select sku, cate, brand, row_number() over(partition by sku order by dt desc)rk from labelx.push_goods
where dt in('2022-03-01', '2022-03-18', '2022-03-31')) t1
where rk = 1)c
on b.sku = c.sku
'''
    file1 = 'data.csv'
    get_data(sql_test, file1, 'config.jsonl', 'user.jsonl', 'item.jsonl')

