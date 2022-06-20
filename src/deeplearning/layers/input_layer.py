from src.deeplearning.feature_column.feature_group import FeatureGroup
from seq_input_layer import SeqInputLayer


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
        self._seq_feature_groups_config = []
        for x in feature_groups_config:
            for y in x.sequence_features:
                self._seq_feature_groups_config.append(y)

        self._group_name_to_seq_features = {
            x.group_name: x.sequence_features for x in feature_groups_config if len(x.sequence_features) > 0
        }

        self._seq_input_layer = None
        if len(self._seq_feature_groups_config) > 0:
            self._seq_input_layer = SeqInputLayer(
                feature_configs,
                self._seq_feature_groups_config,
                use_embedding_variable=use_embedding_variable)

