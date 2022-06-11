from src.pbs import pipeline_pb2
from src.deeplearning.feature_column import SparseFeat, VarLenFeat, DenseFeat, BucketFeat
from src.utils.config_utils import get_configs_from_pipeline_file
from src.deeplearning.data_processing import padding_data, parse_data
import tensorflow as tf


def train(pipeline_config_path):
    # 读取配置文件
    pipeline_config = get_configs_from_pipeline_file(pipeline_config_path)

    # 获取数据config
    data_config = pipeline_config.data_config
    field_delim = data_config.separator
    col_names = [x.input_name for x in data_config.input_fields]
    default_values = [x.default_val for x in data_config.input_fields]
    dtype_dict = {x.input_name: x.input_type for x in data_config.input_fields}
    dtype_map = {
        0: 'INT32',
        1: 'INT64',
        2: 'STRING',
        4: 'FLOAT',
        5: 'DOUBLE',
        6: 'BOOL'
    }
    tf_type_map = {
        0: tf.int32,
        1: tf.int64,
        2: tf.string,
        4: tf.float32,
        5: tf.float32,
        6: tf.bool
    }
    out_type = [tf_type_map[x.input_type] for x in data_config.input_fields]
    if pipeline_config.feature_configs:
        feature_configs = pipeline_config.feature_configs
    else:
        feature_configs = pipeline_config.feature_config.features
    feature_columns = []
    for fc in feature_configs:
        if fc.feature_type == pipeline_pb2.FeatureConfig.FeatureType.SparseFeat and fc.input_names[0] in col_names:
            feature_columns.append(SparseFeat(name=fc.input_names[0],
                                              vocab_size=fc.num_buckets,
                                              hash_size=fc.hash_bucket_size,
                                              share_emb=fc.shared_names,
                                              emb_dim=fc.embedding_dim,
                                              dtype=dtype_map[dtype_dict[fc.input_names[0]]]))
        elif fc.feature_type == pipeline_pb2.FeatureConfig.FeatureType.VarLenFeat and fc.input_names[0] in col_names:
            feature_columns.append(VarLenFeat(name=fc.input_names[0],
                                              vocab_size=fc.num_buckets,
                                              hash_size=fc.hash_bucket_size,
                                              share_emb=fc.shared_names,
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
                                             share_emb=fc.shared_names,
                                             dtype=dtype_map[dtype_dict[fc.input_names[0]]]
                                             ))

        elif fc.feature_type == pipeline_pb2.FeatureConfig.FeatureType.BucketFeat and fc.input_names[0] in col_names:
            feature_columns.append(BucketFeat(name=fc.input_names[0],
                                              boundaries=list(map(float, list(fc.boundaries))),
                                              share_emb=fc.shared_names,
                                              emb_dim=fc.embedding_dim,
                                              dtype=dtype_map[dtype_dict[fc.input_names[0]]]))
    print(feature_columns)

    padding_shape, padding_value = padding_data(feature_columns)

    test_dataset = tf.data.TextLineDataset('D:/BaiduNetdiskDownload/rank_test_data.tsv', num_parallel_reads=4).skip(1)
    test_data = test_dataset.map(lambda x: parse_data(x, col_names, feature_columns, default_values, field_delim, False),
                                 num_parallel_calls=30).padded_batch(padded_shapes=padding_shape,
                                                                                 padding_values=padding_value,
                                                                                 batch_size=5)
    test_data = test_data.prefetch(tf.data.AUTOTUNE)


if __name__ == '__main__':
    train('D:\SS-workspace\文档\deepfm.config')