from digitforce.aip.common.config.config_factory import dg_config_factory

# hive config
HIVE_HOST = dg_config_factory.get_config_value("HIVE_HOST")
HIVE_PORT = dg_config_factory.get_config_value("HIVE_PORT")

# hdfs config
HDFS_HOST = dg_config_factory.get_config_value("HDFS_HOST")
HDFS_PORT = dg_config_factory.get_config_value("HDFS_PORT")

# redis config
REDIS_HOST = dg_config_factory.get_config_value("REDIS_HOST")
REDIS_PORT = dg_config_factory.get_config_value("REDIS_PORT")

# mongodb config
MONGODB_HOST = dg_config_factory.get_config_value("MONGODB_HOST")
MONGODB_PORT = dg_config_factory.get_config_value("MONGODB_PORT")
MONGODB_USER = dg_config_factory.get_config_value("MONGODB_USER")
MONGODB_PASSWORD = dg_config_factory.get_config_value("MONGODB_PASSWORD")
