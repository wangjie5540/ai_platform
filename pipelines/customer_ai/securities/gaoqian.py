import kfp
import kfp.dsl as dsl
from kfp.dsl import Condition

import digitforce.aip.common.utils.kubeflow_helper as kubeflow_helper
from digitforce.aip.common.utils import config_helper
from digitforce.aip.components.feature_engineering import FeatureCreateGaoqian
from digitforce.aip.components.feature_engineering import FeatureCreateGaoqianPredict
from digitforce.aip.components.ml import GaoqianModel
from digitforce.aip.components.ml import GaoqianPredict
from digitforce.aip.components.sample import SampleSelectionGaoqian
from digitforce.aip.components.source.cos import Cos
from kfp.compiler import Compiler

pipeline_name = 'gaoqian_prod'
pipeline_path = f'/tmp/{pipeline_name}.yaml'

@dsl.pipeline(name=pipeline_name)
def ml_gaoqian(global_params: str, flag='TRAIN'):
    RUN_ENV = "prod"
    with Condition(flag != "PREDICT", name="is_not_predict"):
        op_sample_selection = SampleSelectionGaoqian(name='sample_select', global_params=global_params, tag=RUN_ENV)

        op_feature_create = FeatureCreateGaoqian(name='feature_create', global_params=global_params, tag=RUN_ENV,
                                                sample=op_sample_selection.outputs['sample'])

        with Condition(flag == 'TRAIN', name="is_train"):
            op_sample_comb = GaoqianModel(name="model", global_params=global_params, tag=RUN_ENV,
                                         train_data=op_feature_create.outputs[
                                             op_feature_create.OUTPUT_TRAIN_FEATURE
                                         ],
                                         test_data=op_feature_create.outputs[
                                             op_feature_create.OUTPUT_TEST_FEATURE
                                         ])
    with Condition(flag == "PREDICT", name="is_predict"):
        predict_table_op = Cos("predict_cos_url",global_params)
        predict_feature_op = FeatureCreateGaoqianPredict(name="feature_create_predict", global_params=global_params,
                                                        sample=predict_table_op.outputs[Cos.OUTPUT_1],
                                                        tag=RUN_ENV)
        liushi_predict_op = GaoqianPredict(name="model_predict", global_params=global_params, tag=RUN_ENV,
                                          predict_table_name=predict_feature_op.outputs[
                                              predict_feature_op.OUTPUT_PREDICT_FEATURE
                                          ])



import json

global_params = json.dumps({
    "sample_select":
    {
        "train_period": 7,
        "predict_period": 7,
        "event_code": "shengou",
        "category":'simu'
    },
    "model":
    {
        "max_depth": 5,
        "scale_pos_weight": 0.5,
        "n_estimators": 20,
        "learning_rate": 0.05,
        "train_table_name": "algorithm.aip_zq_gaoqian_custom_feature_train",
        "test_table_name": "algorithm.aip_zq_gaoqian_custom_feature_test",
        "model_and_metrics_data_hdfs_path": "/user/ai/aip/model/666"
    },
    "feature_create":
    {
        "train_period": 7,
        "predict_period": 7
    },
    "predict_cos_url":
    {
        "url": "https://algorithm-1308011215.cos.ap-beijing.myqcloud.com/aip_test_lookalike_predict.csv",
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
        "train_period": 7,
        "predict_period": 7
    }
})


kubeflow_helper.upload_pipeline(ml_gaoqian, pipeline_name)
# kubeflow_helper.upload_pipeline_version(ml_gaoqian, kubeflow_helper.get_pipeline_id(pipeline_name),pipeline_name)


kubeflow_config = config_helper.get_module_config("kubeflow")
pipeline_path = f"/tmp/{pipeline_name}.yaml"
pipeline_conf = kfp.dsl.PipelineConf()
pipeline_conf.set_image_pull_policy("Always")
Compiler().compile(pipeline_func=ml_gaoqian, package_path=pipeline_path, pipeline_conf=pipeline_conf)