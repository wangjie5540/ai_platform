import kfp
import kfp.dsl as dsl

from digitforce.aip.components.preprocess import dataset
from digitforce.aip.components.recommend import hot
from digitforce.aip.components.recommend import recall

name = "RecommendMultiRecallAndRank"
description = ""


@dsl.pipeline(
    name=name,
    description=description,
)
def recommend_multi_recall_and_rank_pipeline(run_datetime_str):
    # run_datetime_str = to_date_str(parse_date_str(run_datetime_str))
    # generate hot
    user_show_and_action_table = ""  # 必须包含 user_id, item_id,  click_cnt
    ctr_hot_result = f"/data/recommend/hot/ctr/{run_datetime_str}.csv"  # 无头csv文件  item_id, ctr
    ctr_hot = hot.ctr_hot_op(user_show_and_action_table, ctr_hot_result)  # 计算ctr热门

    click_hot_result = f"/data/recommend/hot/click_cnt/{run_datetime_str}.csv"
    click_hot = hot.click_hot_op(user_show_and_action_table, click_hot_result)  # 计算点击热门

    # user_id,item_id,profile_id,click_cnt,save_cnt,order_cnt,event_timestamp
    user_action_csv_file = f"/data/recommend/user/action/{run_datetime_str}.csv"
    # item2vec
    item2vec_item_emb_jsonl_file = f"/data/recommend/recall/item_emb/item2vec/{run_datetime_str}.jsonl"
    item2vec = recall.item2vec_op(user_action_csv_file, item2vec_item_emb_jsonl_file, vec_size=16)

    # deep fm
    mf_train_dataset_csv_file = f"/data/recommend/dataset/mf/{run_datetime_str}.csv"
    user_and_id_map_file = f"/data/recommend/dataset/user_and_id_map/{run_datetime_str}"
    item_and_id_map_file = f"/data/recommend/dataset/item_and_id_map/{run_datetime_str}"
    deep_mf_dataset = dataset.generate_mf_train_dataset_op(
        user_action_csv_file, mf_train_dataset_csv_file,
        user_and_id_map_file, item_and_id_map_file,
        names="user_id,item_id,profile_id,click_cnt,save_cnt,order_cnt,event_timestamp")
    deep_mf_item_emb_jsonl_file = f"/data/recommend/recall/item_emb/deep_mf/{run_datetime_str}.jsonl"
    deep_mf_user_emb_jsonl_file = f"/data/recommend/recall/user_emb/deep_mf/{run_datetime_str}.jsonl"
    deep_mf = recall.deep_mf_op(mf_train_dataset_csv_file, deep_mf_item_emb_jsonl_file, deep_mf_user_emb_jsonl_file)
    deep_mf.after(deep_mf_dataset)


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
    print(hot.ctr_hot_op)
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
