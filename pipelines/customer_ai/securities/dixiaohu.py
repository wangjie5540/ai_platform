
import digitforce.aip.common.utils.kubeflow_helper as kubeflow_helper
import kfp
import kfp.dsl as dsl

from digitforce.aip.common.utils import config_helper
from digitforce.aip.components.feature_engineering import (
    FeatureCreateDixiaohu,
    FeatureCreateDixiaohuPredict,
)
from digitforce.aip.components.ml import DixiaohuModel, DixiaohuPredict
from digitforce.aip.components.sample import SampleSelectionDixiaohu
from digitforce.aip.components.source.cos import Cos
from kfp.compiler import Compiler
from kfp.dsl import Condition

pipeline_name = "dixiaohu_test"
pipeline_path = f"/tmp/{pipeline_name}.yaml"


@dsl.pipeline(name=pipeline_name)
def ml_dixiaohu(global_params: str, flag="TRAIN"):
    RUN_ENV = "DEV_v4_sd2"
    with Condition(flag != "PREDICT", name="is_not_predict"):
        op_sample_selection = SampleSelectionDixiaohu(
            name="sample_select", global_params=global_params, tag=RUN_ENV
        )

        op_feature_create = FeatureCreateDixiaohu(
            name="feature_create",
            global_params=global_params,
            tag=RUN_ENV,
            sample=op_sample_selection.outputs["sample_table_name"],
        )

        with Condition(flag == "TRAIN", name="is_train"):
            op_sample_comb = DixiaohuModel(
                name="model",
                global_params=global_params,
                tag=RUN_ENV,
                train_table_name=op_feature_create.outputs[
                    op_feature_create.OUTPUT_TRAIN_FEATURE
                ],
                test_table_name=op_feature_create.outputs[
                    op_feature_create.OUTPUT_TEST_FEATURE
                ],
            )
            op_sample_comb.container.set_image_pull_policy("Always")
    with Condition(flag == "PREDICT", name="is_predict"):
        predict_table_op = Cos("predict_cos_url", global_params)

        predict_feature_op = FeatureCreateDixiaohuPredict(
            name="feature_create_predict",
            global_params=global_params,
            sample=predict_table_op.outputs[Cos.OUTPUT_1],
            tag=RUN_ENV,
        )
        dixiaohu_predict_op = DixiaohuPredict(
            name="model_predict",
            global_params=global_params,
            tag=RUN_ENV,
            predict_table_name=predict_feature_op.outputs[
                predict_feature_op.OUTPUT_PREDICT_FEATURE
            ],
        )


# kubeflow_helper.upload_pipeline(ml_dixiaohu, pipeline_name) # 会改变id
kubeflow_helper.upload_pipeline_version(ml_dixiaohu, kubeflow_helper.get_pipeline_id(
    pipeline_name), pipeline_name)  # 更新版本不改变id

# prod环境不同,需采用以下配置生成yaml环境导入
# kubeflow_config = config_helper.get_module_config("kubeflow")
# pipeline_path = f"/tmp/{pipeline_name}.yaml"
# pipeline_conf = dsl.PipelineConf()
# pipeline_conf.set_image_pull_policy("Always")
# Compiler().compile(
#     pipeline_func=ml_dixiaohu,
#     package_path=pipeline_path,
#     pipeline_conf=pipeline_conf,
# )
