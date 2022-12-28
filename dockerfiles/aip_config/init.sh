#!/bin/bash

# 在~/.netrc中添加如下配置
# machine digit-force-generic.pkg.coding.net
# login others-1657101930760
# password f8c1508e03a43feaaedf507bc0e7d60a0c0f0485
if [ $# != 1 ]; then
  echo "USAGE: init.sh dev"
  exit 1
fi

ENVIRONMENT=$1

# 添加sdk自定义配置文件
mkdir -p /usr/local/etc
wget --http-user=others-1657101930760 --http-password=f8c1508e03a43feaaedf507bc0e7d60a0c0f0485 https://digit-force-generic.pkg.coding.net/ai-platform/others/aip_config.yaml?version=$ENVIRONMENT -O /usr/local/etc/.aip_config.yaml
# 添加.kube/config
mkdir -p ~/.kube
wget --http-user=others-1657101930760 --http-password=f8c1508e03a43feaaedf507bc0e7d60a0c0f0485 https://digit-force-generic.pkg.coding.net/ai-platform/others/kube-config?version=$ENVIRONMENT -O ~/.kube/config