import re

from src.pbs import pipeline_pb2


class FeatureGroup(object):

    def __init__(self, feature_group_config):
        self._config = feature_group_config
        assert isinstance(self._config, pipeline_pb2.FeatureGroupConfig)
        assert self._config.wide_deep in (pipeline_pb2.WideOrDeep.WIDE, pipeline_pb2.WideOrDeep.DEEP)

    @property
    def group_name(self):
        return self._config.group_name

    @property
    def wide_and_deep_dict(self):
        wide_and_deep_dict = {}
        for feature_name in self._config.feature_names:
            wide_and_deep_dict[feature_name] = self._config.wide_deep
        return wide_and_deep_dict

    @property
    def feature_names(self):
        return self._config.feature_names

    def select_columns(self, fc):
        if self._config.wide_deep == pipeline_pb2.WideOrDeep.WIDE:
            wide_columns = [fc.wide_columns[x] for x in self._config.feature_names]
            return wide_columns, []
        else:
            sequence_columns = []
            deep_columns = []
            for x in self._config.feature_names:
                if x in fc.sequence_columns:
                    sequence_columns.append(fc.sequence_columns[x])
                else:
                    deep_columns.append(fc.deep_columns[x])
            return deep_columns, sequence_columns
