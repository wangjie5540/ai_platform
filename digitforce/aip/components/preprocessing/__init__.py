# coding: utf-8
import kfp.dsl as dsl

import digitforce.aip.common.utils.component_helper as component_helper

train_data_name = 'train_data'
test_data_name = 'test_data'
user_data_name = 'user_data'
other_data_name = 'other_data'


class SampleCombLookalike(dsl.ContainerOp):
    """
    数据源-读取表组件
    """

    def __init__(self, name, sample, user_feature, item_feature):
        super(SampleCombLookalike, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/preprocessing-sample_comb_lookalike',
            command=['python', 'main.py'],
            arguments=['--sample', sample, '--user_feature', user_feature, '--item_feature', item_feature],
            file_outputs={
                train_data_name: component_helper.generate_output_path(train_data_name),
                test_data_name: component_helper.generate_output_path(test_data_name),
                user_data_name: component_helper.generate_output_path(user_data_name),
                other_data_name: component_helper.generate_output_path(other_data_name)
            }
        )


class ModelFeature2Dataset(dsl.ContainerOp):
    OUTPUT_KEY_TRAIN_DATASET = "train_dataset_table_name"
    OUTPUT_KEY_TEST_DATASET = "test_dataset_table_name"
    def __init__(self, name, global_params, label_table_name, model_user_feature_table_name,
                 model_item_feature_table_name):
        super(ModelFeature2Dataset, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/'
                  f'preprocessing-feature_and_label_to_dataset',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params,
                       '--label_table_name', label_table_name,
                       '--model_user_feature_table_name', model_user_feature_table_name,
                       '--model_item_feature_table_name', model_item_feature_table_name,
                       ],
            file_outputs={
                ModelFeature2Dataset.OUTPUT_KEY_TRAIN_DATASET:
                    component_helper.generate_output_path(ModelFeature2Dataset.OUTPUT_KEY_TRAIN_DATASET),
                ModelFeature2Dataset.OUTPUT_KEY_TEST_DATASET: component_helper.generate_output_path(
                    ModelFeature2Dataset.OUTPUT_KEY_TEST_DATASET),

            }
        )
