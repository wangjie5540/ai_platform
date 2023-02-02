# coding: utf-8
import os

AIP_CONFIG_PATH = '/usr/local/etc/aip_config.yaml'
JSON_OUTPUT_PATH = '/tmp/out'
MOUNT_NFS_DIR = '/mnt/nfs'
IMAGE_REGISTRY = 'digit-force-docker.pkg.coding.net/marketing_algorithm'
REGISTRY_DB = 'hello-world'
# 默认的pvc，需要在部署完毕之后同步创建
DEFAULT_PVC = 'ai-platform-pvc'

AI_PLATFORM_IMAGE_REPO = "digit-force-docker.pkg.coding.net/ai-platform/ai-components"
# spark app name
SPARK_APP_NAME = 'default-name'
# 配置文件挂载地址
CONFIG_MOUNT_PATH = '/mnt/config'

ENV = "DEV"
ENV = os.environ["RUN_ENV"] if os.environ.get("RUN_ENV") else ENV
