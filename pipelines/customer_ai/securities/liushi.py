import kfp
import kfp.dsl as dsl
from kfp.dsl import Condition

import digitforce.aip.common.utils.kubeflow_helper as kubeflow_helper
from digitforce.aip.common.utils import config_helper
from digitforce.aip.components.feature_engineering import FeatureCreateLiushi
from digitforce.aip.components.feature_engineering import FeatureCreateLiushiPredict
from digitforce.aip.components.ml import LiushiModel
from digitforce.aip.components.ml import LiushiPredict
from digitforce.aip.components.sample import SampleSelectionLiushi

pipeline_name = 'liushi'
pipeline_path = f'/tmp/{pipeline_name}.yaml'


@dsl.pipeline(name=pipeline_name)
def ml_liushi(global_params: str, flag='TRAIN', RUN_ENV="dev"):
    op_sample_selection = SampleSelectionLiushi(name='sample_select', global_params=global_params, tag=RUN_ENV)

    op_feature_create = FeatureCreateLiushi(name='feature_create', global_params=global_params, tag=RUN_ENV,
                                            sample=op_sample_selection.outputs['sample_table_name'])

    with Condition(flag == 'TRAIN', name="is_train"):
        op_sample_comb = LiushiModel(name="model", global_params=global_params, tag=RUN_ENV,
                                     train_data=op_feature_create.outputs[
                                         op_feature_create.OUTPUT_TRAIN_FEATURE
                                     ],
                                     test_data=op_feature_create.outputs[
                                         op_feature_create.OUTPUT_TEST_FEATURE
                                     ])
    with Condition(flag == "PREDICT", name="is_predict"):
        predict_feature_op = FeatureCreateLiushiPredict(name="feature_create_predict", global_params=global_params,
                                                        tag=RUN_ENV)
        liushi_predict_op = LiushiPredict(name="model-predict", global_params=global_params, tag=RUN_ENV,
                                          predict_table_name=predict_feature_op.outputs[
                                              predict_feature_op.OUTPUT_PREDICT_FEATURE
                                          ])


kubeflow_config = config_helper.get_module_config("kubeflow")
client = kfp.Client(host="http://172.22.20.9:30000/pipeline", cookies=kubeflow_helper.get_istio_auth_session(
    url=kubeflow_config['url'], username=kubeflow_config['username'],
    password=kubeflow_config['password'])['session_cookie'])
import json

global_params = json.dumps({
    "sample_select": {"active_before_days": 3, "active_after_days": 5, "start_date": "20221211",
                      "end_date": "20221220"},
    "model": {"max_depth": 5, "scale_pos_weight": 0.5, "n_estimators": 200, "learning_rate": 0.05,
              "train_table_name": "algorithm.aip_zq_liushi_custom_feature_train",
              "test_table_name": "algorithm.aip_zq_liushi_custom_feature_test",
              },
    "feature_create": {"active_before_days": 3, "active_after_days": 5, "start_date": "20221211",
                       "end_date": "20221220", "mid_date": "20221218"},

    "model-predict": {
        "predict_table_name": "algorithm.aip_zq_liushi_custom_feature_predict",
        "model_hdfs_path": "/user/ai/aip/zq/liushi/model/lasted.model",
    },

    "feature_create_predict": {"active_before_days": 3, "active_after_days": 5, "start_date": "20221211",
                               "end_date": "20221220", "sample": "algorithm.aip_zq_liushi_custom_predict"},

})
# client.create_run_from_pipeline_func(ml_liushi, arguments={"global_params": global_params},
#                                      experiment_name="recommend",
#                                      namespace='kubeflow-user-example-com')
kubeflow_helper.upload_pipeline(ml_liushi, "liushi-v1.1-dev")
client.create_run_from_pipeline_func(ml_liushi, arguments={"global_params": global_params,
                                                           "flag": "TRAIN"},
                                     experiment_name="recommend",
                                     namespace='kubeflow-user-example-com')

client.create_run_from_pipeline_func(ml_liushi, arguments={"global_params": global_params,
                                                           "flag": "PREDICT"},
                                     experiment_name="recommend",
                                     namespace='kubeflow-user-example-com')

client.create_run_from_pipeline_func(ml_liushi, arguments={"global_params": global_params,
                                                           "flag": "AUTOML"},
                                     experiment_name="recommend",
                                     namespace='kubeflow-user-example-com')
