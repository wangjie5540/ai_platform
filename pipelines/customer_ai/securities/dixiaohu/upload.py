import digitforce.aip.common.utils.kubeflow_helper as kubeflow_helper
import pipeline

# kubeflow_helper.upload_pipeline(pipeline.pipeline_func, pipeline_name=pipeline.pipeline_name) # 会改变id
kubeflow_helper.upload_pipeline_version(
    pipeline_func=pipeline.pipeline_func, 
    pipeline_id= kubeflow_helper.get_pipeline_id(pipeline_name=pipeline.pipeline_name), 
    pipeline_name=pipeline.pipeline_name
)  # 更新版本不改变id

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