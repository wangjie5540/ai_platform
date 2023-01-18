# coding:utf-8

import os
import subprocess
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', type=str, required=True, help='环境')
    parser.add_argument('--name', type=str, required=True, help='组件名称')
    args = parser.parse_args()
    base_image = build_algorithm_base(args.env)
    build_component(base_image, args.name, args.env)
    component_name = os.environ['COMPONENT_NAME']
    component_path = os.path.join('src', *component_name.split('-'))
    os.chdir(component_path)
    cmd = "docker build -t digit-force-docker.pkg.coding.net/ai-platform/ai-components/{component_name} .".format(
        component_name=component_name)
    subprocess.check_call(cmd, shell=True)
    subprocess.check_call(
        "docker login -u ai-components-1672810563540 -p 1d228954e03793ce2e79bf655335abc4e961ec75 digit-force-docker.pkg.coding.net",
        shell=True)
    subprocess.check_call(
        "docker push digit-force-docker.pkg.coding.net/ai-platform/ai-components/{component_name}".format(
            component_name=component_name), shell=True)


def build_algorithm_base(environment):
    algorithm_base_dockerfile = f'''
from digit-force-docker.pkg.coding.net/ai-platform/base-images/algorithm-base:latest
RUN pip install digitforce-aip -i https://aip-1657964384920:546b044f44ad6936fef609faa512a53b3fa8b12f@digit-force-pypi.pkg.coding.net/ai-platform/aip/simple
# 添加配置文件(区分环境)
RUN mkdir -p $ROOT_DIR/.kube && mkdir -p /usr/local/etc
COPY aip_config/{environment}/kube_config $ROOT_DIR/.kube/config
COPY aip_config/{environment}/aip_config.yaml /usr/local/etc
# 添加hive环境配置
COPY aip_config/{environment}/hdfs-site.xml $SPARK_HOME/conf
'''
    with open('Dockerfile', 'w') as f:
        f.write(algorithm_base_dockerfile)
    base_image = f"algorithm-base:{environment}"
    subprocess.check_call(f"docker build -t {base_image} dockerfiles", shell=True)
    return base_image


def build_component(base_image, component_name, environment):
    component_dockerfile = f'''
from {base_image}
ARG COMPONENT_DIR=/component
RUN mkdir -p $COMPONENT_DIR
WORKDIR $COMPONENT_DIR
COPY . $COMPONENT_DIR
'''
    with open('Dockerfile', 'w') as f:
        f.write(component_dockerfile)
    component_path = os.path.join('src', *component_name.split('-'))
    component_image = f"digit-force-docker.pkg.coding.net/ai-platform/ai-components/{component_name}:{environment}"
    build_cmd = f'''
docker build -t {component_image} {component_path}
docker push {component_image}
'''
    subprocess.check_call(build_cmd, shell=True)


if __name__ == '__main__':
    main()
