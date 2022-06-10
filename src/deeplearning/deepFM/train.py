from src.pbs import pipeline_pb2
from src.deeplearning.feature_column import SparseFeat, VarLenFeat, DenseFeat, BucketFeat
from src.utils.config_utils import get_configs_from_pipeline_file
from google.protobuf import text_format


def train(pipeline_config_path):

    # 读取配置文件
    pipeline_config = get_configs_from_pipeline_file(pipeline_config_path)

    # 获取数据config
    data_config = pipeline_config.data_config
    field_delim = data_config.separator
    col_names = [x.input_name for x in data_config.input_fields]
    default_values = [x.default_val for x in data_config.input_fields]
    dtype_dict = {x.input_name: x.input_type for x in data_config.input_fields}
    feature_config = pipeline_config.feature_config
    feature_columns = []
    for fc in feature_config.features:
        if fc.feature_type == pipeline_pb2.FeatureConfig.SparseFeat and fc.input_names in col_names:
            feature_columns.append(SparseFeat(name=fc.input_names,
                                              vocab_size=fc.num_buckets,
                                              hash_size=fc.hash_bucket_size,
                                              share_emb=fc.shared_names,
                                              emb_dim=fc.embedding_dim,
                                              dtype=dtype_dict[fc.input_names]))
        elif fc.feature_type == pipeline_pb2.FeatureConfig.VarLenFeat and fc.input_names in col_names:
            feature_columns.append(VarLenFeat(name=fc.input_names,
                                              vocab_size=fc.num_buckets,
                                              hash_size=fc.hash_bucket_size,
                                              share_emb=fc.shared_names,
                                              seq_multi_sep=fc.seq_multi_sep,
                                              weight_name=fc.weight_name,
                                              emb_dim=fc.embedding_dim,
                                              max_len=fc.sequence_length,
                                              combiner=fc.combiner,
                                              dtype=dtype_dict[fc.input_names],
                                              sub_dtype=fc.sub_field_type
                                              ))
        elif fc.feature_type == pipeline_pb2.FeatureConfig.DenseFeat and fc.input_names in col_names:
            feature_columns.append(DenseFeat(name=fc.input_names,
                                             dim=fc.embedding_dim,
                                             share_emb=fc.shared_names,
                                             dtype=dtype_dict[fc.input_names]
                                             ))

        elif fc.feature_type == pipeline_pb2.FeatureConfig.BucketFeat and fc.input_names in col_names:
            feature_columns.append(BucketFeat(name=fc.input_names,
                                              boundaries=fc.boundaries,
                                              share_emb=fc.shared_names,
                                              emb_dim=fc.embedding_dim,
                                              dtype=dtype_dict[fc.input_names]))

