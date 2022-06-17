from src.deeplearning.feature_column.feature_group import FeatureGroup


class InputLayer(object):

    def __init__(self,
                 feature_configs,
                 feature_groups_config,
                 variational_dropout_config=None,
                 wide_output_dim=-1,
                 use_embedding_variable=False,
                 embedding_regularizer=None,
                 kernel_regularizer=None,
                 is_training=False
                 ):
        self._feature_group = {x.group_name: FeatureGroup(x) for x in feature_groups_config}
