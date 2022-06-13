from src.pbs import pipeline_pb2


def value_transform(val, dtype):
    if dtype == pipeline_pb2.DatasetConfig.FieldType.INT32 or dtype == pipeline_pb2.DatasetConfig.FieldType.INT64:
        return int(val)
    elif dtype == pipeline_pb2.DatasetConfig.FieldType.FLOAT or dtype == pipeline_pb2.DatasetConfig.FieldType.DOUBLE:
        return float(val)
    elif dtype == pipeline_pb2.DatasetConfig.FieldType.BOOL:
        return bool(val)
    else:
        return val
