#!/bin/bash

# 脚本运行要求：设置COMPONENT_NAME环境变量

# 设置镜像仓库默认值
IMAGE_REPO=aip-tcr.tencentcloudcr.com/aip
SCRIPT_DIR=$(cd `dirname $0`; pwd)
if [ $COMPONENT_NAME == "all" ]; then
    echo "编译所有组件"
    components=`cat $SCRIPT_DIR/component_register`
else
    # 把逗号分隔的字符串转换成使用空格分隔
    components=`echo $COMPONENT_NAME | tr ',' ' '`
fi

if [ -z "$TAG" ]; then
    TAG=latest
fi
echo TAG为: $TAG

function build_algorithm_base() {
    echo "FROM $IMAGE_REPO/algorithm-base:latest" > /tmp/Dockerfile
    echo "RUN pip install digitforce-aip -i https://aip-1657964384920:546b044f44ad6936fef609faa512a53b3fa8b12f@digit-force-pypi.pkg.coding.net/ai-platform/aip/simple -U" >> /tmp/Dockerfile
    docker build -t algorithm-base -f /tmp/Dockerfile .
}

function build_component() {
    local base_image=$1
    local component_name=$2
    # 把使用横杆分隔的组件名转换成使用斜杠分隔
    local component_path=src/`echo $component_name | tr '-' '/'`
    local component_image=$IMAGE_REPO/$component_name:$TAG
    pwd
    if [ ! -f $component_path/Dockerfile ]; then
        echo "使用默认的Dockerfile"
        echo "FROM $base_image" > $component_path/Dockerfile
        echo "ARG COMPONENT_DIR=/component" >> $component_path/Dockerfile
        echo "RUN mkdir -p \$COMPONENT_DIR" >> $component_path/Dockerfile
        echo "WORKDIR \$COMPONENT_DIR" >> $component_path/Dockerfile
        echo "COPY . \$COMPONENT_DIR" >> $component_path/Dockerfile
    fi
    if [ -f $component_path/requirements.txt ]; then
        echo "RUN pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple" >> /tmp/Dockerfile
    fi
    docker build -t $component_image -f $component_path/Dockerfile $component_path
    docker push $component_image
}

build_algorithm_base
echo $components

for component in $components; do
    echo "开始编译组件：$component"
    build_component algorithm-base $component
    echo "编译组件：$component 完成"
    echo "----------------------------------------"
done