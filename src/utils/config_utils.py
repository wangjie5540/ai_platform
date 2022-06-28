import tensorflow as tf
from google.protobuf import text_format, json_format
from src.pbs import pipeline_pb2


def get_configs_from_pipeline_file(pipeline_config_path):

    """Reads config from a file containing pipeline_pb2.RecConfig.
    Args:
        pipeline_config_path: Path to pipeline_pb2.RecConfig text
            proto.

    Returns:
        Dictionary of configuration objects. Keys are `model`, `train_config`,
        `train_input_config`, `eval_config`, `eval_input_config`. Value are the
        corresponding config objects.
    """

    if isinstance(pipeline_config_path, pipeline_pb2.RecConfig):
        return pipeline_config_path

    assert tf.io.gfile.exists(
        pipeline_config_path
    ), 'pipeline_config_path [%s] not exists' % pipeline_config_path

    pipeline_config = pipeline_pb2.RecConfig()
    with tf.io.gfile.GFile(pipeline_config_path, 'r') as f:
        config_str = f.read()
        if pipeline_config_path.endswith('.config'):
            text_format.Merge(config_str, pipeline_config)
        elif pipeline_config_path.endswith('.json'):
            json_format.Parse(config_str, pipeline_config)
        else:
            assert False, 'invalid file format(%s), currently support formats: .config(prototxt) .json'\
                          % pipeline_config_path

    return pipeline_config
