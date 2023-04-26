import digitforce.aip.common.utils.kubeflow_helper as kubeflow_helper
import pipeline

with open('global_params.json', 'r') as f:
    kubeflow_helper.create_run_directly(
        pipeline_name=pipeline.pipeline_name,
        pipeline_func=pipeline.pipeline_func,
        experiment_name='default',
        arguments={
            'global_params': f.read(),
            'flag': 'Predict',
        }
    )
