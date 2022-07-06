#!/bin/sh
BASE_DIR=/data/server
APP_NAME=${rootArtifactId}
cd $BASE_DIR
sh $APP_NAME/bin/run.sh stop
rm -rf $APP_NAME
tar -zxvf $APP_NAME-bin.tar.gz
rm -rf $APP_NAME-bin.tar.gz
cd $APP_NAME
mkdir logs
sh bin/run.sh start