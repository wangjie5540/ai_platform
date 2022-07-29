import sys
import argparse
import pandas as pd


def get_samples_train(cur_str, train_date, target_cates, predict_date, label_event: str):
    if label_event == '购买':
        sql = f'''
            select 
                        t1.user_id,
                        if(t2.user_id is null, 0, 1) as label
                    from
                    (
                        select 
                            a.user_id as user_id
                        from
                        (select 
                            user_id as user_id
                        from
                            labelx.push_traffic_tmp
                        where 
                            event_time  between '{train_date}' and '{cur_str}'
                        group by 
                            user_id
                        ) as a
                        left join 
                        (select 
                            b.user_id
                        from
                        (select 
                            user_id as user_id,
                            sku as sku
                        from
                            labelx.push_order
                        where
                            order_timebetween '{train_date}' and '{cur_str}'
                        group by 
                            user_id, sku) as b
                        left join
                        (select 
                            sku as sku,
                            cate as cat
                        from 
                            labelx.push_goods
                        where dt= '{cur_str}'
                            and cate in {target_cates})as c
                        on b.sku=c.sku
                        where c.sku is not null
                        group by b.user_id) as d
                        on a.user_id = d.user_id
                        where d.user_id is null
                    ) as t1
                    left join
                    (   
                        select e.user_id as user_id from
                        (select 
                            user_id as user_id,
                            sku as sku
                        from
                            labelx.push_order
                        where
                            order_time> '{cur_str}' and order_time<= '{predict_date}'
                        group by user_id, sku)as e
                        left join
                        (select 
                            sku as sku,
                            cate as cat
                        from 
                            labelx.push_goods
                        where dt= '{predict_date}'
                            and cate in {target_cates})as f
                        on e.sku=f.sku
                        where f.sku is not null
                        group by e.user_id
                    )as t2
                    on t1.user_id = t2.user_id
        '''
    else:
        if label_event == '点击':
            label_event = 'CLICK'
        elif label_event == '加购':
            label_event = 'CART_ADD'
        elif label_event == '浏览':
            label_event = 'BROWSE'
        elif label_event == '收藏':
            label_event = 'COLLECT'

        sql = f'''
                    select 
                        a.user_id,
                        if(b.user_id is null, 0, 1) as label
                    from
                    (select
                        t1.user_id as user_id
                    from
                        (select 
                            user_id as user_id
                        from
                            labelx.push_traffic_tmp
                        where 
                            event_time between '{train_date}' and '{cur_str}'
                        group by 
                            user_id
                        ) as t1
                        left join
                        (select c.user_id from
                        (select 
                            user_id as user_id,
                            sku as sku
                        from
                            labelx.push_traffic_tmp
                        where
                            event_time between '{train_date}' and '{cur_str}'
                            and event_code = '{label_event}'
                        group by 
                            user_id, sku) as c
                        left join
                        (select 
                            sku as sku,
                            cate as cat
                        from 
                            labelx.push_goods
                        where dt= '{cur_str}'
                            and cate in {target_cates})as d
                        on c.sku=d.sku
                        where d.sku is not null
                        group by c.user_id) as t2
                        on t1.user_id = t2.user_id
                        where t2.user_id is null
                    ) as a
                    left join
                    (   
                        select e.user_id as user_id from
                        (select 
                            user_id as user_id,
                            sku as sku
                        from
                            labelx.push_traffic_tmp
                        where
                            event_time > '{cur_str}' and event_time <= '{predict_date}'
                            and event_code = '{label_event}'
                        group by user_id, sku)as e
                        left join
                        (select 
                            sku as sku,
                            cate as cat
                        from 
                            labelx.push_goods
                        where dt= '{predict_date}'
                            and cate in {target_cates})as f
                        on e.sku=f.sku
                        where f.sku is not null
                        group by e.user_id
                    )as b
                    on a.user_id = b.user_id
                '''
    return sql


def get_samples_predict(target_cates, train_date, cur_str, label_event):
    if label_event == '购买':
        sql = f'''
            select 
                        t1.user_id as user_id
                    from
                    (
                        select 
                            a.user_id as user_id
                        from
                        (select 
                            user_id as user_id
                        from
                            labelx.push_traffic_tmp
                        where 
                            event_time  between '{train_date}' and '{cur_str}'
                        group by 
                            user_id
                        ) as a
                        left join 
                        (select 
                            b.user_id
                        from
                        (select 
                            user_id as user_id,
                            sku as sku
                        from
                            labelx.push_order
                        where
                            order_timebetween '{train_date}' and '{cur_str}'
                        group by 
                            user_id, sku) as b
                        left join
                        (select 
                            sku as sku,
                            cate as cat
                        from 
                            labelx.push_goods
                        where dt= '{cur_str}'
                            and cate in {target_cates})as c
                        on b.sku=c.sku
                        where c.sku is not null
                        group by b.user_id) as d
                        on a.user_id = d.user_id
                        where d.user_id is null
                    ) as t1              
        '''
    else:
        if label_event == '点击':
            label_event = 'CLICK'
        elif label_event == '加购':
            label_event = 'CART_ADD'
        elif label_event == '浏览':
            label_event = 'BROWSE'
        elif label_event == '收藏':
            label_event = 'COLLECT'
        sql = f'''
               select 
                        a.user_id
                    from
                    (select
                        t1.user_id as user_id
                    from
                        (select 
                            user_id as user_id
                        from
                            labelx.push_traffic_tmp
                        where 
                            event_time between '{train_date}' and '{cur_str}'
                        group by 
                            user_id
                        ) as t1
                        left join
                        (select c.user_id from
                        (select 
                            user_id as user_id,
                            sku as sku
                        from
                            labelx.push_traffic_tmp
                        where
                            event_time between '{train_date}' and '{cur_str}'
                            and event_code = '{label_event}'
                        group by 
                            user_id, sku) as c
                        left join
                        (select 
                            sku as sku,
                            cate as cat
                        from 
                            labelx.push_goods
                        where dt= '{cur_str}'
                            and cate in {target_cates})as d
                        on c.sku=d.sku
                        where d.sku is not null
                        group by c.user_id) as t2
                        on t1.user_id = t2.user_id
                        where t2.user_id is null
                    ) as a    
                '''
    return sql

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--train', type=bool, default=False, help='train or not')
    parser.add_argument('--predict', type=bool, default=False, help='predict or not')
    parser.add_argument('--train_date', type=str, required=True, help='from train date to current is training period')
    parser.add_argument('--current_date', type=str, required=True, help='current date represented as a string')
    parser.add_argument('--predict_date', type=str, help='from current date to predict date is predicting(observing) period')
    parser.add_argument('--target_cates', type=str, help='categories used for high-potential, represented as a string, split with , ')
    parser.add_argument('--event', type=str, help='event used to define what is high potential candidates, including purchase, click, collect, cart_add, browse')
    args = parser.parse_args()
    if args.train:
        sql = get_samples_train(args.current_date, args.train_date, args.target_cates, args.predict_date, args.event)

    if args.predict:
        sql = get_samples_predict(args.target_cates, args.train_date, args.current_date, args.event)

if __name__ == '__main__':
    main()