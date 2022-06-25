import logging
from collections import namedtuple

from tensorflow import feature_column

from src.pbs import pipeline_pb2
from src.pbs.pipeline_pb2 import WideOrDeep
from src.utils.proto_utils import copy_obj
from src.deeplearning.layers.utils import build_initializer


SparseFeat = namedtuple('SparseFeat',
                        ['name', 'vocab_size', 'hash_size', 'share_emb', 'emb_dim', 'dtype'])
VarLenFeat = namedtuple('VarLenFeat',
                        ['name', 'vocab_size', 'hash_size', 'share_emb', 'seq_multi_sep', 'weight_name', 'emb_dim',
                         'max_len', 'combiner', 'dtype', 'sub_dtype'])
DenseFeat = namedtuple('DenseFeat',
                       ['name', 'dim', 'share_emb', 'dtype'])
BucketFeat = namedtuple('BucketFeat',
                        ['name', 'boundaries', 'share_emb', 'emb_dim', 'dtype'])

dtype_map = {
    0: 'int32',
    1: 'int64',
    2: 'string',
    4: 'float64',
    5: 'float64',
    6: 'bool'
}


def build_feature_columns(feature_configs, dtype_dict, col_names):
    feature_columns = []
    for fc in feature_configs:
        if fc.feature_type == pipeline_pb2.FeatureConfig.FeatureType.SparseFeat and fc.input_names[0] in col_names:
            feature_columns.append(SparseFeat(name=fc.input_names[0],
                                              vocab_size=fc.num_buckets,
                                              hash_size=fc.hash_bucket_size,
                                              share_emb=fc.shared_names[0] if fc.shared_names else '',
                                              emb_dim=fc.embedding_dim,
                                              dtype=dtype_map[dtype_dict[fc.input_names[0]]]))
        elif fc.feature_type == pipeline_pb2.FeatureConfig.FeatureType.VarLenFeat and fc.input_names[0] in col_names:
            feature_columns.append(VarLenFeat(name=fc.input_names[0],
                                              vocab_size=fc.num_buckets,
                                              hash_size=fc.hash_bucket_size,
                                              share_emb=fc.shared_names[0] if fc.shared_names else '',
                                              seq_multi_sep=fc.seq_multi_sep,
                                              weight_name=fc.weight_name,
                                              emb_dim=fc.embedding_dim,
                                              max_len=fc.sequence_length,
                                              combiner=fc.combiner,
                                              dtype=dtype_map[dtype_dict[fc.input_names[0]]],
                                              sub_dtype=dtype_map[fc.sub_field_type]
                                              ))
        elif fc.feature_type == pipeline_pb2.FeatureConfig.FeatureType.DenseFeat and fc.input_names[0] in col_names:
            feature_columns.append(DenseFeat(name=fc.input_names[0],
                                             dim=fc.embedding_dim,
                                             share_emb=fc.shared_names[0] if fc.shared_names else '',
                                             dtype=dtype_map[dtype_dict[fc.input_names[0]]]
                                             ))

        elif fc.feature_type == pipeline_pb2.FeatureConfig.FeatureType.BucketFeat and fc.input_names[0] in col_names:
            feature_columns.append(BucketFeat(name=fc.input_names[0],
                                              boundaries=list(map(float, list(fc.boundaries))),
                                              share_emb=fc.shared_names[0] if fc.shared_names else '',
                                              emb_dim=fc.embedding_dim,
                                              dtype=dtype_map[dtype_dict[fc.input_names[0]]]))


class FeatureKeyError(KeyError):

  def __init__(self, feature_name):
    super(FeatureKeyError, self).__init__(feature_name)


class SharedEmbedding(object):

  def __init__(self, embedding_name, index, sequence_combiner=None):
    self.embedding_name = embedding_name
    self.index = index
    self.sequence_combiner = sequence_combiner


class FeatureColumnParser(object):
    """Parse and generate feature columns."""

    def __init__(self, feature_configs, wide_deep_dict=None, wide_output_dim=-1, use_embedding_variable=False):
        if wide_deep_dict is None:
            wide_deep_dict = {}
        self._feature_configs = feature_configs
        self._wide_deep_dict = wide_deep_dict
        self._wide_output_dim = wide_output_dim
        self._use_embedding_variable = use_embedding_variable

        self._deep_columns = {}
        self._wide_columns = {}
        self._sequence_columns = {}
        self._share_embed_names = {}
        self._share_embed_infos = {}
        self._vocab_size = {}

        def _cmp_embed_config(a, b):
            return a.embedding_dim == b.embedding_dim and a.combiner == b.combiner and\
                a.initializer == b.initializer and a.max_partitions == b.max_partitions and\
                a.use_embedding_variable == b.use_embedding_variable

        for config in self._feature_configs:
            if not config.HasField('embedding_name'):
                continue
            embed_name = config.embedding_name

            if embed_name in self._share_embed_names:
                assert _cmp_embed_config(config, self._share_embed_infos[embed_name]), \
                    'shared embed info of [%s] is not matched [%s] vs [%s]' % (
                        embed_name, config, self._share_embed_infos[embed_name])
                self._share_embed_names[embed_name] += 1
            else:
                self._share_embed_names[embed_name] = 1
                self._share_embed_infos[embed_name] = copy_obj(config)

        # remove not shared embedding names
        not_shared = [
            x for x in self._share_embed_names if self._share_embed_names[x] == 1
        ]
        for embed_name in not_shared:
            del self._share_embed_names[embed_name]
            del self._share_embed_infos[embed_name]

        logging.info('shared embeddings[num=%d]' % len(self._share_embed_names))
        for embed_name in self._share_embed_names:
            logging.info('\t%s: share_num[%d], share_info[%s]' %
                         (embed_name, self._share_embed_names[embed_name],
                          self._share_embed_infos[embed_name]))
        self._deep_share_embed_columns = {
            embed_name: [] for embed_name in self._share_embed_names
        }
        self._wide_share_embed_columns = {
            embed_name: [] for embed_name in self._share_embed_names
        }
        # TODO
        for config in self._feature_configs:
            pass

    def parse_sparse_feature(self, config):
        hash_bucket_size = config.hash_bucket_size
        if hash_bucket_size > 0:
            fc = feature_column.categorical_column_with_hash_bucket(config.input_name[0], hash_bucket_size)
        elif config.vocab_list:
            fc = feature_column.categorical_column_with_vocabulary_list(config.input_name[0],
                                                                        default_value=0,
                                                                        vocabulary_list=config.vocab_list)
        elif config.vocab_file:
            fc = feature_column.categorical_column_with_vocabulary_file(config.input_name[0],
                                                                        default_value=0,
                                                                        vocabulary_file=config.vocab_file)
        else:
            fc = feature_column.categorical_column_with_identity(config.input_name[0],
                                                                 num_buckets=config.num_buckets,
                                                                 default_value=0)
        if self.is_wide(config):
            self._add_wide_embedding_column(fc, config)
        if self.is_deep(config):
            self._add_deep_embedding_column(fc, config)

    #TODO
    def parse_var_len_features(self, config):
        pass

    def is_wide(self, config):
        feature_name = config.feature_name if config.HasField('feature_name') else config.input_name[0]

        if feature_name not in self._wide_deep_dict:
            raise FeatureKeyError(feature_name)
        return self._wide_deep_dict[feature_name] in [WideOrDeep.WIDE, WideOrDeep.WIDE_AND_DEEP]

    def is_deep(self, config):
        feature_name = config.feature_name if config.HasField('feature_name') else config.input_name[0]

        if feature_name not in self._wide_deep_dict:
            raise FeatureKeyError(feature_name)
        return self._wide_deep_dict[feature_name] in [WideOrDeep.DEEP, WideOrDeep.WIDE_AND_DEEP]

    def _add_wide_embedding_column(self, fc, config):
        feature_name = config.feature_name if config.HasField('feature_name') else config.input_name[0]
        assert self._wide_output_dim > 0, 'wide_output_dim is not set'
        if config.embedding_name in self._wide_share_embed_columns:
            wide_fc = self._add_shared_embedding_column(config.embedding_name, fc, deep=False)
        else:
            initializer = None
            if config.HasField('initializer'):
                initializer = build_initializer(config.initializer)
            wide_fc = feature_column.embedding_column(
                fc,
                self._wide_output_dim,
                combiner='sum',
                initializer=initializer)
        self._wide_columns[feature_name] = wide_fc

    def _add_deep_embedding_column(self, fc, config):
        feature_name = config.feature_name if config.HasField('feature_name') else config.input_name[0]
        assert config.embedding_dim > 0, 'embedding_dim is not set for %s' % feature_name
        if config.embedding_name in self._deep_share_embed_columns:
            fc = self._add_shared_embedding_column(config.embedding_name, fc, deep=True)
        else:
            initializer = None
            if config.HasField('initializer'):
                initializer = build_initializer(config.initializer)
            fc = feature_column.embedding_column(
                fc,
                config.embedding_dim,
                combiner=config.combiner,
                initializer=initializer
            )
        if config.feature_type != config.SequenceFeature:
            self._deep_columns[feature_name] = fc
        else:
            if config.HasField('sequence_combiner'):
                fc.sequence_combiner = config.sequence_combiner
            self._sequence_columns[feature_name] = fc

    def _add_shared_embedding_column(self, embedding_name, fc, deep=False):

        if deep:
            curr_id = len(self._deep_share_embed_columns[embedding_name])
            self._deep_share_embed_columns[embedding_name].append(fc)
        else:
            curr_id = len(self._wide_share_embed_columns[embedding_name])
            self._wide_share_embed_columns[embedding_name].append(fc)
        return SharedEmbedding(embedding_name, curr_id, None)







