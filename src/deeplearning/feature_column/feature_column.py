from collections import namedtuple
from src.pbs import pipeline_pb2


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


def build_feature_columns(feature_configs, dtype_dict):
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