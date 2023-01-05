import kfp
import kfp.dsl as dsl

import digitforce.aip.common.utils.kubeflow_helper as kubeflow_helper
from digitforce.aip.common.utils import config_helper
from digitforce.aip.components.feature_engineering import FeatureCreateLiushiPredict
from digitforce.aip.components.ml import LiushiPredict

pipeline_name = 'liushi_predict'
pipeline_path = f'/tmp/{pipeline_name}.yaml'


@dsl.pipeline(name=pipeline_name)
def ml_liushi_predict(global_params: str, flag='TRAIN'):
    predict_feature_op = FeatureCreateLiushiPredict(name="feature_create_predict", global_params=global_params)
    liushi_predict_op = LiushiPredict(name="model-predict", global_params=global_params,
                                      predict_table_name=predict_feature_op.outputs[
                                          predict_feature_op.OUTPUT_PREDICT_FEATURE
                                      ])


kubeflow_config = config_helper.get_module_config("kubeflow")
client = kfp.Client(host="http://172.22.20.9:30000/pipeline", cookies=kubeflow_helper.get_istio_auth_session(
    url=kubeflow_config['url'], username=kubeflow_config['username'],
    password=kubeflow_config['password'])['session_cookie'])
import json

global_params = json.dumps({
    "model-predict": {
        "predict_table_name": 3,
        "model_hdfs_path": 5,
    },

    "feature_create_predict": {"active_before_days": 3, "active_after_days": 5, "start_date": "20221211",
                               "end_date": "20221220", "sample": "algorithm.aip_zq_liushi_custom_predict"},

})
client.create_run_from_pipeline_func(ml_liushi_predict, arguments={"global_params": global_params},
                                     experiment_name="recommend",
                                     namespace='kubeflow-user-example-com')
