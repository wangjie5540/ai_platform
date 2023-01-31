# coding:utf-8

import os
import subprocess

component_list = [
    'demo-print',
]


def main():
    # 配合coding进行构建
    component_name = os.environ['COMPONENT_NAME']
    # dev | test | uat | prod
    environment = os.environ['ENVIRONMENT']
    base_image = build_algorithm_base(environment)
    if component_name == 'all':
        for component_name in component_list:
            build_component(base_image, component_name, environment)
    else:
        for component_name in component_name.strip().split(','):
            build_component(base_image, component_name, environment)


def build_algorithm_base(environment):
    algorithm_base_dockerfile = '''
from digit-force-docker.pkg.coding.net/ai-platform/base-images/algorithm-base:latest
RUN pip install digitforce-aip -i https://aip-1657964384920:546b044f44ad6936fef609faa512a53b3fa8b12f@digit-force-pypi.pkg.coding.net/ai-platform/aip/simple
# 添加配置挂载映射
# 参考: https://spark.apache.org/docs/latest/running-on-kubernetes.html#configuration
ENV KUBECONFIG /usr/local/etc/kube_config
# 参考: https://spark.apache.org/docs/latest/configuration.html
ENV SPARK_CONF_DIR /usr/local/etc
'''.format(environment=environment)
    with open('Dockerfile', 'w') as f:
        f.write(algorithm_base_dockerfile)
    base_image = "algorithm-base:{environment}".format(environment=environment)
    cmd = "docker build -t {base_image} -f {cur_dir}/Dockerfile dockerfiles".format(base_image=base_image,
                                                                                    cur_dir=os.curdir)
    print(cmd)
    subprocess.check_call(cmd, shell=True)
    return base_image


def build_component(base_image, component_name, environment):
    component_dockerfile = '''
from {base_image}
ARG COMPONENT_DIR=/component
RUN mkdir -p $COMPONENT_DIR
WORKDIR $COMPONENT_DIR
COPY . $COMPONENT_DIR
'''.format(base_image=base_image)
    with open('Dockerfile', 'w') as f:
        f.write(component_dockerfile)
    component_path = os.path.join('src', *component_name.split('-'))
    component_image = "digit-force-docker.pkg.coding.net/ai-platform/ai-components/{component_name}:{environment}".format(
        component_name=component_name, environment=environment)
    build_cmd = '''
docker build -t {component_image} -f {cur_dir}/Dockerfile {component_path}
docker login -u ai-components-1672810563540 -p 1d228954e03793ce2e79bf655335abc4e961ec75 digit-force-docker.pkg.coding.net
docker push {component_image}
'''.format(component_image=component_image, cur_dir=os.curdir, component_path=component_path)
    print(build_cmd)
    subprocess.check_call(build_cmd, shell=True)


if __name__ == '__main__':
    main()
