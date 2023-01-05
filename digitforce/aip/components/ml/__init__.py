# coding: utf-8
import kfp.dsl as dsl
import digitforce.aip.common.utils.component_helper as component_helper


class Lookalike(dsl.ContainerOp):
    def __init__(self, name, global_params, train_dataset, test_dataset):
        super(Lookalike, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/'
                  f'ml-lookalike',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params, '--train_data', train_dataset,
                       '--test_data', test_dataset, ],
            file_outputs={
            }
        )


class LookalikeModel(dsl.ContainerOp):
    def __init__(self, name, global_params, train_dataset_table_name, test_dataset_table_name):
        super(LookalikeModel, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/'
                  f'ml-lookalike',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params,
                       '--train_dataset_table_name', train_dataset_table_name,
                       '--test_dataset_table_name', test_dataset_table_name,
                       ],
            file_outputs={
            }
        )


class Liushi(dsl.ContainerOp):
    def __init__(self, name, global_params, train_data, test_data,):
        super(Lookalike, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/ml-lookalike',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params, '--train_data', train_data,
                       '--test_data', test_data],
            file_outputs={
            }
        )