# coding: utf-8
import kfp.dsl as dsl
import digitforce.aip.common.utils.component_helper as component_helper


class Lookalike(dsl.ContainerOp):
    def __init__(self, name, global_params, train_data, test_data, user_data, other_data):
        super(Lookalike, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/ml-lookalike',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params, '--train_data', train_data,
                       '--test_data', test_data, '--user_data', user_data, '--other_data', other_data],
            file_outputs={
            }
        )


class Gaoqian(dsl.ContainerOp):
    def __init__(self, name, global_params, train_data, test_data, other_data):
        super(Gaoqian, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/ml-gaoqian',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params, '--train_data', train_data,
                       '--test_data', test_data, '--other_data', other_data],
            file_outputs={
            }
        )