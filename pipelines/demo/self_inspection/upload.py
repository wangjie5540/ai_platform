import digitforce.aip.common.utils.kubeflow_helper as kubeflow_helper
import pipeline

res = kubeflow_helper.upload_pipeline(
    pipeline.pipeline_func,
    pipeline_name=pipeline.pipeline_name
)
print(res)
