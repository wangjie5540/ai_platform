from digitforce.aip.components.demo.print import DemoPrint
from digitforce.aip.components.demo.sleep import DemoSleep
from digitforce.aip.components.source.cos import Cos
from digitforce.aip.components.demo.starrocks import DemoStarrocks
import kfp

pipeline_name = 'self_inspection'


@kfp.dsl.pipeline(name=pipeline_name)
def pipeline_func(global_params):
    print_op = DemoPrint(name='demo_print', global_params=global_params, tag='latest')
    sleep_op = DemoSleep(name='demo_sleep', global_params=global_params, tag='latest')
    read_cos_op = Cos(name='demo_read_cos', global_params=global_params, tag='latest')
    read_starrocks_op = DemoStarrocks(name="demo_read_starrocks", global_params=global_params, tag='latest')
