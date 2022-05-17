from digitforce.aip.common.config.config_factory import dg_config_factory

# hive config
HIVE_HOST = dg_config_factory.get_config_value("HIVE_HOST")
HIVE_PORT = dg_config_factory.get_config_value("HIVE_PORT")

# hdfs config
HDFS_HOST = dg_config_factory.get_config_value("HDFS_HOST")
HDFS_PORT = dg_config_factory.get_config_value("HDFS_PORT")
