from collections import OrderedDict

from tensorflow import feature_column

from src.deeplearning.feature_column.feature_group import FeatureGroup
from src.pbs.pipeline_pb2 import WideOrDeep
from src.deeplearning.feature_column.feature_column import FeatureColumnParser


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
        self._feature_groups = {x.group_name: FeatureGroup(x) for x in feature_groups_config}
        self._seq_feature_groups_config = []
        for x in feature_groups_config:
            for y in x.sequence_features:
                self._seq_feature_groups_config.append(y)

        self._group_name_to_seq_features = {
            x.group_name: x.sequence_features for x in feature_groups_config if len(x.sequence_features) > 0
        }

        self._seq_input_layer = None
        # TODO
        # if len(self._seq_feature_groups_config) > 0:
        #     self._seq_input_layer = SeqInputLayer(
        #         feature_configs,
        #         self._seq_feature_groups_config,
        #         use_embedding_variable=use_embedding_variable)

        wide_and_deep_dict = self.get_wide_deep_dict()
        self._fc_parser = FeatureColumnParser(feature_configs,
                                              wide_and_deep_dict,
                                              wide_output_dim,
                                              use_embedding_variable=use_embedding_variable)

    def has_group(self, group_name):
        return group_name in self._feature_groups

    def __call__(self, features, group_name, is_combine=True):
        """Get features by group_name.

            Args:
                features: input tensor dict
                group_name: feature_group name
                is_combine: whether to combine sequence features over the
                    time dimension.

            Return:
                features: all features concatenate together
                group_features: list of features
                seq_features: list of sequence features, each element is a tuple:
                    3 dimension embedding tensor (batch_size, max_seq_len, embedding_dimension),
                    1 dimension sequence length tensor.
        """
        assert group_name in self._feature_groups, \
            f"invalid group_name{group_name}, list:{','.join([x for x in self._feature_groups])}"

    def sing_call_input_layer(self, features, group_name, is_combine=True, feature_name_to_output_tensors=None):
        """Get features by group_name.

        Args:
          features: input tensor dict
          group_name: feature_group name
          is_combine: whether to combine sequence features over the
              time dimension.
          feature_name_to_output_tensors: if set sequence_features, feature_name_to_output_tensors will
          take key tensors to reuse.

        Return:
          features: all features concatenate together
          group_features: list of features
          seq_features: list of sequence features, each element is a tuple:
              3 dimension embedding tensor (batch_size, max_seq_len, embedding_dimension),
              1 dimension sequence length tensor.
        """
        assert group_name in self._feature_groups, \
            f"invalid group_name{group_name}, list:{','.join([x for x in self._feature_groups])}"
        feature_group = self._feature_groups[group_name]
        group_columns, group_seq_columns = feature_group.select_columns(self._fc_parser)

        # TODO
        if is_combine:
            cols_to_output_tensors = OrderedDict()


    def get_wide_deep_dict(self):
        """Get wide or deep indicator for feature columns.

            Returns:
              dict of { feature_name : WideOrDeep }
        """
        wide_and_deep_dict = {}
        for fg_name in self._feature_groups.keys():
            fg = self._feature_groups[fg_name]
            tmp_dict = fg.wide_and_deep_dict
            for k in tmp_dict:
                v = tmp_dict[k]
                if k not in wide_and_deep_dict:
                    wide_and_deep_dict[k] = v
                elif wide_and_deep_dict[k] != v:
                    wide_and_deep_dict[k] = WideOrDeep.WIDE_AND_DEEP
                else:
                    pass
        return wide_and_deep_dict



