import json
import os
import logging


from digitforce.aip.common.hive_helper import df_hive_helper
from digitforce.aip.common.redis_client import RedisClient


def get_data(sql, output_file):

    logging.info("read data from hive")
    df = df_hive_helper.query_to_df(sql)

    logging.info("start data processing")
    redis_conn = RedisClient('172.21.32.143')

    config_data = json.loads(redis_conn.get_redis_string('rank_feature_config').decode())

    df['sex'] = df['sex'].apply(lambda x: config_data['gender'].get(x, 0))
    df['province'] = df['province'].apply(lambda x: config_data['province'].get(x, 0))
    df['city'] = df['city'].apply(lambda x: config_data['city'].get(x, 0))
    df['cate'] = df['cate'].apply(lambda x: config_data['cate'].get(x, 0))
    df['brand'] = df['brand'].apply(lambda x: config_data['brand'].get(x, 0))

    columns = ['sex', 'age', 'province', 'city', 'life_stage', 'consume_level',
               'membership_level', 'cate', 'brand', 'label']
    df = df[columns]
    logging.info("data processing end")

    dir_name = os.path.dirname(output_file)
    logging.info(f"dir_name: {dir_name}")
    if dir_name and not os.path.exists(dir_name):
        logging.info(f"mkdir -p {dir_name}")
        os.system(f"mkdir -p {dir_name}")

    logging.info(f"write data to {output_file}")
    df.to_csv(output_file, index=False)


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
    file = 'data.csv'
    get_data(sql_test, file)

