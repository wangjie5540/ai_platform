# -*- coding: utf-8 -*-
# @Time : 2022/05/24
# @Author : Arvin

def update_shop_task(shop_status_table_name, task_instance_table_name):
    """
    df:pandas 当天所有的任务实例
    col_lable:需要更新的列
    update_type:更新方式 insert or update
    实现两个功能：
    1.新任务以shop为单位插入表 shop表
    2.输出当日要预测df
    from shop_status_table_name get shop_df(老的任务)
    from task_instance_table_name get all tasks_instance（今天下发的任务实例）
     new_tasks = []
     for taskid in tasks_instance:
       if taskid not in shop_df['task_id']:
         is_new_task=1
         shops = get_shops(taskid)
         for shop_id in shops:
           if pred_granularity  in shop_df['pred_granularity']:
             is_new_granularity = 0
           else:
             is_new_granularity = 1
             is_history = 1
           if shop_id in shop_df['shop']:
             is_new_shop=0
           else:
             is_new_shop=1
             is_history = 1
           if   pred_granularity[2] in ['week','month']:
             is_preds_by_wm=1
           else:
             is_preds_by_wm=0
           insert into  shop_status_table_name(shop,pred_granularity,is_new_shop,is_new_granularity	is_preds_by_wm)
           new_tasks.append(taskid)
     old_task = tasks_instance[(tasks_instance['taks_id'] not in new_tasks) or (is_preds_by_wm==1)]

    """
    return old_task

def update_table(table_name,col_key,col_value,condition=''):

    """
        UPDATE table SET {col_key}={col_value} where 1=1 {condition}

    
    """
