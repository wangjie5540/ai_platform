# coding: utf-8
import subprocess
import os
from facade.components.ml.uplift import *

full_image_name = f'{image_name}:{image_tag}'
# 如果是本地测试，需要把WORKSPACE指定为代码的跟目录
# export WORKSPACE=/Users/wangtonggui/Desktop/work_code/digitforce-ai-platform
current_dir = os.path.dirname(__file__)
dockerfile_dir = os.path.join(current_dir, 'Dockerfile')
workspace = os.getenv('WORKSPACE')

build_cmd = f'docker build -t {full_image_name} -f {dockerfile_dir} {workspace}'
push_cmd = f'docker push {image_full_name}'

subprocess.check_call(build_cmd, shell=True)
subprocess.check_call(push_cmd, shell=True)
