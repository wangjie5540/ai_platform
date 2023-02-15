import kfp
import kfp.dsl as dsl
from kfp.dsl import Condition
import os
os.environ['PYTHONPATH'] = '/data/pycharm_project_666/digitforce-ai-platform'
print('[[[[[')
import digitforce.aip.common.utils.kubeflow_helper as kubeflow_helper
from digitforce.aip.common.utils import config_helper
from digitforce.aip.components.feature_engineering import FeatureCreateGaoqian
from digitforce.aip.components.feature_engineering import FeatureCreateGaoqianPredict
from digitforce.aip.components.ml import GaoqianModel
from digitforce.aip.components.ml import GaoqianPredict
from digitforce.aip.components.sample import SampleSelectionGaoqian
from digitforce.aip.components.source.cos import Cos
from kfp.compiler import Compiler

pipeline_name = 'gaoqian'
pipeline_path = f'/tmp/{pipeline_name}.yaml'

@dsl.pipeline(name=pipeline_name)
def ml_gaoqian(global_params: str, flag='TRAIN'):
    RUN_ENV = "prod"
    with Condition(flag != "PREDICT", name="is_not_predict"):
        op_sample_selection = SampleSelectionGaoqian(name='sample_select', global_params=global_params, tag=RUN_ENV)
        op_sample_selection.container.set_image_pull_policy("Always")

        op_feature_create = FeatureCreateGaoqian(name='feature_create', global_params=global_params, tag=RUN_ENV,
                                                sample=op_sample_selection.outputs['sample'])
        op_feature_create.container.set_image_pull_policy("Always")

        with Condition(flag == 'TRAIN', name="is_train"):
            op_sample_comb = GaoqianModel(name="model", global_params=global_params, tag=RUN_ENV,
                                         train_data=op_feature_create.outputs[
                                             op_feature_create.OUTPUT_TRAIN_FEATURE
                                         ],
                                         test_data=op_feature_create.outputs[
                                             op_feature_create.OUTPUT_TEST_FEATURE
                                         ])
            op_sample_comb.container.set_image_pull_policy("Always")
    with Condition(flag == "PREDICT", name="is_predict"):
        predict_table_op = Cos("predict_cos_url",global_params)
        predict_table_op.container.set_image_pull_policy("Always")
        predict_feature_op = FeatureCreateGaoqianPredict(name="feature_create_predict", global_params=global_params,
                                                        sample=predict_table_op.outputs[Cos.OUTPUT_1],
                                                        tag=RUN_ENV)
        predict_feature_op.container.set_image_pull_policy("Always")
        liushi_predict_op = GaoqianPredict(name="model_predict", global_params=global_params, tag=RUN_ENV,
                                          predict_table_name=predict_feature_op.outputs[
                                              predict_feature_op.OUTPUT_PREDICT_FEATURE
                                          ])
        liushi_predict_op.container.set_image_pull_policy("Always")


client = kfp.Client(host="http://172.22.20.9:30000/pipeline", cookies=kubeflow_helper.get_istio_auth_session(
    url="http://172.22.20.9:30000/pipeline", username="admin@example.com",
    password="password")['session_cookie'])
import json

global_params = json.dumps({
    "sample_select":
    {
        "train_period": 30,
        "predict_period": 30,
        "event_code": "fund_buy",
        "category":'混合型'
    },
    "model":
    {
        "max_depth": 5,
        "scale_pos_weight": 0.5,
        "n_estimators": 20,
        "learning_rate": 0.05,
        "train_table_name": "algorithm.tmp_aip_train_data_gaoqian",
        "test_table_name": "algorithm.tmp_aip_test_data_gaoqian",
        "model_and_metrics_data_hdfs_path": "/user/ai/aip/model/666"
    },
    "feature_create":
    {
        "event_code": "fund_buy",
        "category":'混合型'
    },
    "predict_cos_url":
    {
        "url": "https://algorithm-1308011215.cos.ap-beijing.myqcloud.com/1675411629205-liushi.csv",
        "columns": "custom_id"
    },
    "model_predict":
    {
        "predict_table_name": "algorithm.tmp_aip_user_feature_gaoqian_predict",
        "model_hdfs_path": "/user/ai/aip/zq/gaoqian/model/lasted.model",
        "output_file_name": "10000-1621413913913888769.csv"
    },
    "feature_create_predict":
    {
        "event_code": "fund_buy",
        "category":'混合型'
    }
})


# kubeflow_helper.upload_pipeline(ml_loss_warning, pipeline_name)
# kubeflow_helper.upload_pipeline_version(ml_loss_warning, kubeflow_helper.get_pipeline_id(pipeline_name),pipeline_name)
client.create_run_from_pipeline_func(ml_gaoqian, arguments={"global_params": global_params,
                                                           "flag": "TRAIN"},
                                     experiment_name="recommend",
                                     namespace='kubeflow-user-example-com')
#
# client.create_run_from_pipeline_func(ml_loss_warning, arguments={"global_params": global_params,
#                                                            "flag": "PREDICT"},
#                                      experiment_name="recommend",
#                                      namespace='kubeflow-user-example-com')
#
# client.create_run_from_pipeline_func(ml_loss_warning, arguments={"global_params": global_params,
#                                                            "flag": "AUTOML"},
#                                      experiment_name="recommend",
#                                      namespace='kubeflow-user-example-com')


kubeflow_config = config_helper.get_module_config("kubeflow")
pipeline_path = f"/tmp/{pipeline_name}.yaml"
pipeline_conf = kfp.dsl.PipelineConf()
pipeline_conf.set_image_pull_policy("Always")
Compiler().compile(pipeline_func=ml_gaoqian, package_path=pipeline_path, pipeline_conf=pipeline_conf)