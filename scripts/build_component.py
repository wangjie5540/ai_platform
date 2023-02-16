# coding:utf-8

import os
import subprocess

"""
本脚本仅用于coding上的CI构建组件(尚未在Windows上测试)
"""

component_list = [
    'demo-print',
]


def main():
    # 配合coding进行构建
    component_name = os.environ['COMPONENT_NAME']
    tag = os.getenv('TAG', 'latest')
    base_image = build_algorithm_base()
    if component_name == 'all':
        for component_name in component_list:
            build_component(base_image, component_name, tag)
    else:
        for component_name in component_name.strip().split(','):
            build_component(base_image, component_name, tag)


def build_algorithm_base():
    algorithm_base_dockerfile = '''
FROM digit-force-docker.pkg.coding.net/ai-platform/base-images/algorithm-base:latest
RUN pip install digitforce-aip -i https://aip-1657964384920:546b044f44ad6936fef609faa512a53b3fa8b12f@digit-force-pypi.pkg.coding.net/ai-platform/aip/simple
'''
    with open('Dockerfile', 'w') as f:
        f.write(algorithm_base_dockerfile)
    base_image = "algorithm-base"
    cmd = "docker build -t {base_image} -f {cur_dir}/Dockerfile dockerfiles".format(
        base_image=base_image,
        cur_dir=os.curdir
    )
    print(cmd)
    subprocess.check_call(cmd, shell=True)
    return base_image


def build_component(base_image, component_name, tag='latest'):
    # 获取组件路径
    component_path = os.path.join('src', *component_name.split('-'))
    # 获取编译命令
    component_image = "digit-force-docker.pkg.coding.net/ai-platform/ai-components/{component_name}:{tag}".format(
        component_name=component_name, tag=tag)
    # 查询是否存在自定制的Dockerfile，如果存在则使用自定义的Dockerfile
    customized_dockerfile = os.path.join(component_path, 'Dockerfile')
    if not os.path.exists(customized_dockerfile):
        component_dockerfile = '''
from {base_image}
ARG COMPONENT_DIR=/component
RUN mkdir -p $COMPONENT_DIR
WORKDIR $COMPONENT_DIR
COPY . $COMPONENT_DIR
'''.format(base_image=base_image)
        with open(customized_dockerfile, 'w') as f:
            f.write(component_dockerfile)
    requirements_path = os.path.join(component_path, 'requirements.txt')
    if os.path.exists(requirements_path):
        with open(customized_dockerfile, 'a') as f:
            f.write('RUN pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple')
    print(build_component_cmd(component_image, component_path))
    subprocess.check_call(build_component_cmd(component_image, component_path), shell=True)


def build_component_cmd(component_image, component_path):
    return '''
docker build -t {component_image} -f {component_path}/Dockerfile {component_path} && docker login -u ai-components-1672810563540 -p 1d228954e03793ce2e79bf655335abc4e961ec75 digit-force-docker.pkg.coding.net && docker push {component_image}
    '''.format(component_image=component_image, component_path=component_path)


if __name__ == '__main__':
    main()
