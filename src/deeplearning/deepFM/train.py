from src.pbs import pipeline_pb2
from google.protobuf import text_format


def train(pipeline_config_path):

    # 读取配置文件
    with open(pipeline_config_path, 'r') as f:
        config_str = f.read()
    pipeline_config = pipeline_pb2.RecConfig()
    text_format.Merge(config_str, pipeline_config)

    # 读取训练数据
    train_input_path = text_format.train_input_path
    default_value = []
