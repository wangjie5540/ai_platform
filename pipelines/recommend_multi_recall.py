import os
import kfp
import kfp.dsl as dsl

from digitforce.aip.components.preprocess import dataset
from digitforce.aip.components.preprocess import sql
from digitforce.aip.components.recommend import hot
from digitforce.aip.components.recommend import recall
from digitforce.aip.components.recommend.tmp import rank_data_process_op, lightgbm_train_op, jsonl_to_mongo
from digitforce.aip.components.source import df_model_manager
from digitforce.aip.components.source import hive

name = "RecommendMultiRecallAndRankMongo"
description = '''{"source": [{"labelx.push_user": ["user_id", "gender", "age", "click_cnt", "city"]}, {"labelx.push_goods": ["sku", "category_l", "category_m", "category_s", "order_cnt"]}, {"labelx.push_traffic_behavior": ["event_time", "event_code", "user_id", "sku"]}]}'''


@dsl.pipeline(
    name=name,
    description=description,
)
def recommend_multi_recall_and_rank_pipeline(train_data_start_date_str, train_data_end_date_str, run_datetime_str,
                                             solution_id, instance_id):
    # run_datetime_str = to_date_str(parse_date_str(run_datetime_str))
    show_and_action_sql = f'''
    SELECT user_id, 
        sku AS item_id, 
        1 AS show_cnt,
        category_level_3 AS profile_id,
        IF(event_code='CLICK', 1, 0) AS click_cnt,  
        IF(event_code='CART_ADD', 1, 0) AS save_cnt,  
        0 AS order_cnt,  
        unix_timestamp(event_time) AS event_timestamp 
         
    FROM labelx.push_traffic_behavior 
    WHERE dt <= '{train_data_end_date_str}' AND dt >= '{train_data_start_date_str}' 
        AND event_code IN ('EXPOSURE','CLICK', 'CART_ADD')
    
    '''
    user_show_and_action_table = f"aip.show_and_action_latest"  # 必须包含 user_id, item_id,  click_cnt
    show_and_action_table_maker = sql.hive_sql_executor(show_and_action_sql, user_show_and_action_table)
    show_and_action_table_maker.container.set_image_pull_policy("Always")

    # generate hot
    ctr_hot_result = f"/data/recommend/hot/ctr/{run_datetime_str}.csv"  # 无头csv文件  item_id, ctr
    ctr_hot = hot.ctr_hot_op(user_show_and_action_table, ctr_hot_result) \
        .after(show_and_action_table_maker)
    ctr_hot.container.set_image_pull_policy("Always")  # 计算ctr热门

    click_hot_result = f"/data/recommend/hot/click_cnt/{run_datetime_str}.csv"
    click_hot = hot.click_hot_op(user_show_and_action_table, click_hot_result).after(
        show_and_action_table_maker)
    click_hot.container.set_image_pull_policy("Always")  # 计算点击热门

    # user_id,item_id,profile_id,click_cnt,save_cnt,order_cnt,event_timestamp
    user_action_csv_file = f"/data/recommend/user/action/{run_datetime_str}.csv"
    user_action_csv_sql = f'''
    SELECT user_id, item_id, profile_id, click_cnt, save_cnt, order_cnt, event_timestamp 
    FROM {user_show_and_action_table} 
    WHERE click_cnt > 0 OR save_cnt > 0 OR order_cnt > 0 
    '''
    user_action_loader = hive.query_to_csv_op(user_action_csv_sql, user_action_csv_file) \
        .after(show_and_action_table_maker)
    user_action_loader.container.set_image_pull_policy("Always")

    # item2vec
    item2vec_item_emb_jsonl_file = f"/data/recommend/recall/item_emb/item2vec/{run_datetime_str}.jsonl"
    item2vec_item_emb_jsonl_hdfs_path = f"/user/aip/recommend/recall/item_emb/item2vec/{run_datetime_str}.jsonl"
    item2vec_recall_result_jsonl_file = f"/data/recommend/recall/recall_result/item2vec/{run_datetime_str}.jsonl"
    item2vec = recall.item2vec_op(user_action_csv_file, item2vec_item_emb_jsonl_file,
                                  recall_result_file=item2vec_recall_result_jsonl_file, vec_size=16) \
        .after(user_action_loader)
    item2vec.container.set_image_pull_policy("Always")

    item2vec_recall_to_redis = recall.upload_recall_result_op(item2vec_recall_result_jsonl_file,
                                                              "AIP_RC_RECALL_item2vec__item2vec__").after(item2vec)
    item2vec_recall_to_redis.container.set_image_pull_policy("Always")

    upload_item2vec_item_emb = df_model_manager.save_one_model_to_model_manage_system(
        solution_id, instance_id, item2vec_item_emb_jsonl_file, item2vec_item_emb_jsonl_hdfs_path,
        model_name="word2vec_item_emb").after(item2vec_recall_to_redis)
    upload_item2vec_item_emb.container.set_image_pull_policy("Always")

    # deep fm
    mf_train_dataset_csv_file = f"/data/recommend/dataset/mf/{run_datetime_str}.csv"
    user_and_id_map_file = f"/data/recommend/dataset/user_and_id_map/{run_datetime_str}"
    item_and_id_map_file = f"/data/recommend/dataset/item_and_id_map/{run_datetime_str}"
    deep_mf_dataset = dataset.generate_mf_train_dataset_op(
        user_action_csv_file, mf_train_dataset_csv_file,
        user_and_id_map_file, item_and_id_map_file,
        names="") \
        .after(user_action_loader)
    deep_mf_dataset.container.set_image_pull_policy("Always")
    deep_mf_item_emb_jsonl_file = f"/data/recommend/recall/item_emb/deep_mf/{run_datetime_str}.jsonl"
    deep_mf_user_emb_jsonl_file = f"/data/recommend/recall/user_emb/deep_mf/{run_datetime_str}.jsonl"
    deep_mf_user_emb_jsonl_hdfs = f"/user/aip/recommend/recall/user_emb/deep_mf/{run_datetime_str}.jsonl"
    deep_mf = recall.deep_mf_op(mf_train_dataset_csv_file, deep_mf_item_emb_jsonl_file, deep_mf_user_emb_jsonl_file)
    deep_mf.after(deep_mf_dataset)
    deep_mf.container.set_image_pull_policy("Always")
    deep_mf_recall_result_jsonl_file = f"/data/recommend/recall/recall_result/deep_mf/{run_datetime_str}.jsonl"
    deep_mf_recall = recall.similarity_search_recall_op(deep_mf_user_emb_jsonl_file,
                                                        deep_mf_item_emb_jsonl_file,
                                                        deep_mf_recall_result_jsonl_file,
                                                        2000,
                                                        user_and_id_map_file,
                                                        item_and_id_map_file).after(deep_mf)
    deep_mf_recall.container.set_image_pull_policy("Always")
    deep_mf_recall_to_redis = recall.upload_recall_result_op(deep_mf_recall_result_jsonl_file,
                                                             "AIP_RC_RECALL_deep_mf__deep_mf__").after(deep_mf_recall)
    deep_mf_recall_to_redis.container.set_image_pull_policy("Always")

    upload_deepmf_item_emb = df_model_manager.save_one_model_to_model_manage_system(
        solution_id, instance_id, deep_mf_item_emb_jsonl_file, deep_mf_user_emb_jsonl_hdfs,
        model_name="deep_mf_item_emb").after(deep_mf_recall_to_redis)
    upload_deepmf_item_emb.container.set_image_pull_policy("Always")

    #### rank ###
    dataset_file_path = f"/data/recommend/rank/lgb/dataset/train/{run_datetime_str}.csv"
    _sql = f'''select a.user_id,sex, age, life_stage, consume_level, province, city, membership_level,  b.sku, cate, brand, label from 
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
    info_log_file = f"/data/recommend/rank/log/lgb/{run_datetime_str}.log"
    error_log_file = f"/data/recommend/rank/log/lgb/{run_datetime_str}.error"
    user_features_file = f"/data/recommend/rank/log/lgb/{run_datetime_str}/{solution_id}/{instance_id}/user_features.jsonl"
    item_features_file = f"/data/recommend/rank/log/lgb/{run_datetime_str}/{solution_id}/{instance_id}/item_features.jsonl"
    config_file = f"/data/recommend/rank/log/lgb/{run_datetime_str}/{solution_id}/{instance_id}/config.jsonl"
    data_process_op = rank_data_process_op(_sql, dataset_file_path, info_log_file, error_log_file, config_file,
                                           user_features_file, item_features_file)
    model_path = f"/data/recommend/rank/lgb/model/{run_datetime_str}"
    user_features_to_mongo_op = jsonl_to_mongo(mode=1, db_name='recommend', collection='rank_user_features',
                                               file_name=user_features_file, info_log_file=info_log_file,
                                               error_log_file=error_log_file, cond_key='user_id').after(data_process_op)
    user_features_to_mongo_op.set_image_pull_policy("Always")
    item_features_to_mongo_op = jsonl_to_mongo(mode=1, db_name='recommend', collection='rank_goods_features',
                                               file_name=user_features_file, info_log_file=info_log_file,
                                               error_log_file=error_log_file, cond_key='sku').after(data_process_op)
    item_features_to_mongo_op.set_image_pull_policy("Always")
    features_config_to_mongo_op = jsonl_to_mongo(mode=1, db_name='recommend', collection='features_config',
                                                 file_name=config_file, info_log_file=info_log_file,
                                                 error_log_file=error_log_file, cond_key='sku').after(data_process_op)
    features_config_to_mongo_op.set_image_pull_policy("Always")
    train_op = lightgbm_train_op(dataset_file_path, model_path, info_log_file, error_log_file).after(data_process_op)

    model_hdfs_path = f"/user/aip/recommend/rank/lgb/{run_datetime_str}"
    upload_model_op = df_model_manager.save_models_to_model_manage_system(
        solution_id, instance_id, model_path, model_hdfs_path, 'lgb_model').after(train_op)
    upload_model_op.container.set_image_pull_policy("Always")


def upload_pipeline():
    pipeline_path = "/data/aip/pipelines/" + name + ".yaml"
    kfp.compiler.Compiler().compile(recommend_multi_recall_and_rank_pipeline, pipeline_path)
    client = kfp.Client()

    client.upload_pipeline(pipeline_path, name, description)


def main():
    # client = kfp.Client()
    # client.create_run_from_pipeline_func(recommend_multi_recall_and_rank_pipeline, arguments={},
    #                                      namespace='kubeflow-user-example-com')
    # client = kfp.Client()

    upload_pipeline()


if __name__ == '__main__':
    main()
    # from kubernetes import client
    # from kserve import KServeClient
    # from kserve import constants
    # from kserve import utils
    # from kserve import V1beta1InferenceService
    # from kserve import V1beta1InferenceServiceSpec
    # from kserve import V1beta1PredictorSpec
    # from kserve import V1beta1SKLearnSpec
    #
    # name = 'sklearn-iris'
    # kserve_version = 'v1beta1'
    # api_version = constants.KSERVE_GROUP + '/' + kserve_version
    #
    # isvc = V1beta1InferenceService(api_version=api_version,
    #                                kind=constants.KSERVE_KIND,
    #                                metadata=client.V1ObjectMeta(
    #                                    name=name, namespace=namespace,
    #                                    annotations={'sidecar.istio.io/inject': 'false'}),
    #                                spec=V1beta1InferenceServiceSpec(
    #                                    predictor=V1beta1PredictorSpec(
    #                                        sklearn=(V1beta1SKLearnSpec(
    #                                            storage_uri="gs://kfserving-samples/models/sklearn/iris"))))
    #                                )
