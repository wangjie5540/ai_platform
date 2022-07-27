import sys
import argparse
import pandas as pd


def get_samples_train(cur_str, train_date, predict_date, cate_list):
    sql = f'''
        select 
            a.user_id,
            if(b.user_id is null, 0, 1) as label
        from
        (
            select c.user_id as user_id from
            (select 
                user_id,
                sku
            from
                labelx.push_order
            where
                order_time between '{train_date}' and '{cur_str}'
            group by user_id,sku)as c
            left join
            (select 
                sku,
                cat
            from 
                labelx.push_goods
            where dt = '{cur_str}'
                and cate in {cate_list})as d
            on c.sku=d.sku
            where d.sku is not null
            group by c.user_id
        ) as a
        left join
        (   
            select e.user_id as user_id from
            (select 
                user_id,
                sku
            from
                labelx.push_order
            where
                dt > '{cur_str}' and dt <= '{predict_date}'
            group by user_id, sku)as e
            left join
            (select 
                sku,
                cat
            from 
                labelx.push_goods
            where dt = '{predict_date}'
                and cate in {cate_list})as f
            on e.sku=f.sku
            where f.sku is not null
            group by e.user_id
        )as b
        on a.user_id = b.user_id
    '''
    return sql

def get_samples_predict(cur_str, train_date, cate_list):
    sql = f'''
         select 
            c.user_id as user_id 
        from
            (select 
                user_id,
                sku
            from
                labelx.push_user
            where
                order_time between '{train_date}' and '{cur_str}'
            group by user_id,sku)as c
            left join
            (select 
                sku,
                cat
            from 
                labelx.push_goods
            where dt = '{cur_str}'
                and cate in {cate_list})as d
            on c.sku=d.sku
            where d.sku is not null
            group by c.user_id   
    '''
    return sql

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--train', type=bool, default=False, help='train or not')
    parser.add_argument('--predict', type=bool, default=False, help='predict or not')
    parser.add_argument('--train_date', type=str, required=True, help='from train date to current is training period')
    parser.add_argument('--current_date', type=str, required=True, help='current date represented as a string')
    parser.add_argument('--predict_date', type=str, help='from current date to predict date is predicting(observing) period')
    parser.add_argument('--cate_list', type=str, help='categories used for repurchase, represented as a string, split with , ')

    args = parser.parse_args()
    if args.train:
        sql = get_samples_train(args.current_date, args.train_date, args.predict_date, args.cate_list)

    if args.predict:
        sql = get_samples_predict(args.current_date, args.train_date, args.cate_list)


if __name__ == '__main__':
    main()