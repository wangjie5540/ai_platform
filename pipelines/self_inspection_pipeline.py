import os

import digitforce.aip.common.utils.kubeflow_helper as kubeflow_helper
from digitforce.aip.components.demo.print import DemoPrint
from digitforce.aip.components.demo.sleep import DemoSleep
from digitforce.aip.components.source.cos import Cos
from digitforce.aip.components.demo.starrocks import DemoStarrocks
import kfp

pipeline_name = 'self_inspection'
pipeline_path = f'/tmp/{pipeline_name}.yaml'


def pipeline_func(global_params):
    print_op = DemoPrint(name='demo_print', global_params=global_params, tag='latest')
    sleep_op = DemoSleep(name='demo_sleep', global_params=global_params, tag='latest')
    read_cos_op = Cos(name='demo_read_cos', global_params=global_params, tag='latest')
    read_starrocks_op = DemoStarrocks(name="demo_read_starrocks", global_params=global_params, tag='latest')


with open('inspection_global_params.json', 'r') as f:
    kubeflow_helper.create_run_directly(
        pipeline_func=pipeline_func,
        experiment_name='default',
        arguments={
            'global_params': f.read()
        })

# res = kubeflow_helper.upload_pipeline(pipeline_func, pipeline_name=pipeline_name)
# print(res)
