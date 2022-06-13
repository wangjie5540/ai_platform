from src.pbs import pipeline_pb2
from src.deeplearning.feature_column import SparseFeat, VarLenFeat, DenseFeat, BucketFeat
from src.utils.config_utils import get_configs_from_pipeline_file
from src.deeplearning.data_processing import padding_data, parse_data
from src.utils.format_transform import value_transform
import tensorflow as tf
from src.deeplearning.deepFM.deepFM import deepfm


def train(pipeline_config_path):
    # 读取配置文件
    pipeline_config = get_configs_from_pipeline_file(pipeline_config_path)

    # 获取数据config
    data_config = pipeline_config.data_config
    field_delim = data_config.separator
    col_names = [x.input_name for x in data_config.input_fields]
    default_values = [[value_transform(x.default_val, x.input_type)] for x in data_config.input_fields]

    dtype_dict = {x.input_name: x.input_type for x in data_config.input_fields}
    dtype_map = {
        0: 'int32',
        1: 'int64',
        2: 'string',
        4: 'float64',
        5: 'float64',
        6: 'bool'
    }
    tf_type_map = {
        0: tf.int32,
        1: tf.int64,
        2: tf.string,
        4: tf.float32,
        5: tf.float32,
        6: tf.bool
    }

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


    padding_shape, padding_value = padding_data(feature_columns)
    batch_size = data_config.batch_size
    prefetch_size = data_config.prefetch_size
    shuffle_buffer_size = data_config.shuffle_buffer_size
    train_input_path = pipeline_config.train_input_path
    test_input_path = pipeline_config.test_input_path
    test_dataset = tf.data.TextLineDataset(test_input_path, num_parallel_reads=4).skip(1)
    test_data = test_dataset.map(lambda x: parse_data(x, col_names, feature_columns, default_values),
                                 num_parallel_calls=30)\
        .padded_batch(padded_shapes=padding_shape,
                                                                     padding_values=padding_value,
                                                                     batch_size=batch_size)
    test_data = test_data.prefetch(tf.data.AUTOTUNE)
    train_dataset = tf.data.TextLineDataset(train_input_path, num_parallel_reads=20).skip(
        1)
    train_data = train_dataset.map(lambda x: parse_data(x, col_names, feature_columns, default_values),
                                   num_parallel_calls=60).shuffle(shuffle_buffer_size).padded_batch(padded_shapes=padding_shape,
                                                                                        padding_values=padding_value,
                                                                                        batch_size=batch_size)
    train_data = train_data.prefetch(tf.data.AUTOTUNE)

    model_config = pipeline_config.model_config

    deep_col_name = [x.feature_names for x in model_config.feature_groups if x.group_name == 'deep'][0]
    wide_col_name = [x.feature_names for x in model_config.feature_groups if x.group_name == 'wide'][0]
    model_dir = pipeline_config.model_dir
    model = deepfm(feature_columns, wide_col_name, deep_col_name, l2_reg=1e-4,
                   dropout_rate=0.2)
    lr_schedule = tf.keras.optimizers.schedules.ExponentialDecay(0.001, decay_steps=100000, decay_rate=0.8,
                                                                 staircase=True)
    model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=lr_schedule),
                  loss='binary_crossentropy', metrics=tf.metrics.AUC(name='auc'))
    model.fit(train_data, epochs=10, validation_data=test_data, verbose=1)

    tf.saved_model.save(model, model_dir)


if __name__ == '__main__':
    train('D:Downloads/deepfm.config')
