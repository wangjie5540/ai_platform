import digitforce.aip.common.utils.kubeflow_helper as kubeflow_helper
import pipeline

kubeflow_helper.upload_pipeline(pipeline.pipeline_func, pipeline_name=pipeline.pipeline_name) # 会改变id