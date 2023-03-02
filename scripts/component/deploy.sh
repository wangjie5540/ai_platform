#!/bin/bash

# 脚本运行要求：设置COMPONENT_NAME环境变量

# 设置镜像仓库默认值
IMAGE_REPO=aip-tcr.tencentcloudcr.com/aip
SCRIPT_DIR=$(cd `dirname $0`; pwd)
# 把逗号分隔的字符串转换成使用空格分隔
components=`echo $COMPONENT_NAME | tr ',' ' '`

if [ -z "$TAG" ]; then
    TAG=latest
fi
echo TAG为: $TAG

function build_component() {
    local component_name=$1
    # 把使用横杆分隔的组件名转换成使用斜杠分隔
    local component_path=src/`echo $component_name | tr '-' '/'`
    local component_image=$IMAGE_REPO/$component_name:$TAG
    echo component_image: $component_image
    component_dockerfile=scripts/build_component/Dockerfile
    if [ ! -f $component_path/Dockerfile ]; then
        component_dockerfile=$component_path/Dockerfile
    else
        component_dockerfile=scripts/component/Dockerfile
    fi
    ls -l $component_path/requirements.txt
    if [ -f $component_path/requirements.txt ]; then
        # 文件中添加一个空行
        echo "" >> $component_dockerfile
        echo "RUN pip install -r requirements.txt -i https://aip-1657964384920:546b044f44ad6936fef609faa512a53b3fa8b12f@digit-force-pypi.pkg.coding.net/ai-platform/aip/simple" >> $component_dockerfile
    fi
    echo component_dockerfile: $component_dockerfile
    cat $component_dockerfile
    docker build -t $component_image -f $component_dockerfile $component_path
    docker push $component_image
}

echo $components

for component in $components; do
    echo "开始编译组件：$component"
    build_component $component
    echo "编译组件：$component 完成"
    echo "----------------------------------------"
done