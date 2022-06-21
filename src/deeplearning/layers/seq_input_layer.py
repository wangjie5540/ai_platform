import tensorflow as tf

from src.pbs import pipeline_pb2


class SeqInputLayer(object):

    def __init__(self, feature_config, feature_groups_config, use_embedding_variable=False):
        self._feature_groups_config = {
            x.group_name: x for x in feature_groups_config
        }
        wide_and_deep_dict = self.get_wide_deep_dict()


    def get_wide_deep_dict(self):
        wide_and_deep_dict = {}
        for group_name_config in self._feature_groups_config.values():
            for x in group_name_config.seq_att_map:
                for key in x.key:
                    wide_and_deep_dict[key] = pipeline_pb2.FeatureGroupConfig.WideOrDeep.DEEP
                for hist_seq in x.hist_seq:
                    wide_and_deep_dict[hist_seq] = pipeline_pb2.FeatureGroupConfig.WideOrDeep.DEEP
        return wide_and_deep_dict

