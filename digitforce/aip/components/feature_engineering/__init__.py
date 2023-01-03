# coding: utf-8
import kfp.dsl as dsl

import digitforce.aip.common.utils.component_helper as component_helper


class FeatureCreateLookalike(dsl.ContainerOp):
    OUTPUT_USER_FEATURE = 'user_feature'
    OUTPUT_ITEM_FEATURE = 'item_feature'

    def __init__(self, name, global_params, sample):
        super(FeatureCreateLookalike, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/feature_engineering-feature_create_lookalike',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params, '--sample', sample],
            file_outputs={
                self.OUTPUT_USER_FEATURE: component_helper.generate_output_path(self.OUTPUT_USER_FEATURE),
                self.OUTPUT_ITEM_FEATURE: component_helper.generate_output_path(self.OUTPUT_ITEM_FEATURE)
            }
        )

class FeatureCreateLiushi(dsl.ContainerOp):
    OUTPUT_TRAIN_FEATURE = 'train_feature_table_name'
    OUTPUT_TEST_FEATURE = 'train_feature_table_name'

    def __init__(self, name, global_params, sample):
        super(FeatureCreateLookalike, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/ai-platform/ai-components/feature_engineering-feature_create_liushi',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params, '--sample', sample],
            file_outputs={
                self.OUTPUT_TRAIN_FEATURE: component_helper.generate_output_path(self.OUTPUT_TRAIN_FEATURE),
                self.OUTPUT_TEST_FEATURE: component_helper.generate_output_path(self.OUTPUT_TEST_FEATURE)
            }
        )


class RawUserFeatureOp(dsl.ContainerOp):
    OUTPUT_RAW_USER_FEATURE = 'raw_user_feature'

    def __init__(self, name, global_params):
        super(RawUserFeatureOp, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/'
                  f'ai-platform/ai-components/feature_engineering-raw_user_feature',
            arguments=[
                '--name', name,
                '--global_params', global_params,
            ],
            file_outputs={
                self.OUTPUT_RAW_USER_FEATURE: component_helper.generate_output_path(self.OUTPUT_RAW_USER_FEATURE)
            }
        )


class RawItemFeatureOp(dsl.ContainerOp):
    OUTPUT_RAW_ITEM_FEATURE = 'raw_item_feature'

    def __init__(self, name, global_params):
        super(RawItemFeatureOp, self).__init__(
            name=name,
            image=f'digit-force-docker.pkg.coding.net/'
                  f'ai-platform/ai-components/feature_engineering-raw_item_feature',
            command=['python', 'main.py'],
            arguments=['--name', name, '--global_params', global_params],
            file_outputs={
                self.OUTPUT_RAW_ITEM_FEATURE: component_helper.generate_output_path(self.OUTPUT_RAW_ITEM_FEATURE)
            }
        )
