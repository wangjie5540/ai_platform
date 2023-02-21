# TODO 尚未完成
# spark-k8s-runtime:3.1.2镜像是通过spark-3.1.2-bin-hadoop2.7打包获得，命令如下：
# docker build -t digit-force-docker.pkg.coding.net/ai-platform/base-images/spark-k8s-runtime:3.1.2 -f  kubernetes/dockerfiles/spark/Dockerfile  .
FROM digit-force-docker.pkg.coding.net/ai-platform/base-images/spark-k8s-runtime:3.2.1
ARG SPARK_JARS=/opt/spark/jars
ARG WGET_COMMAND="wget --http-user=others-1657101930760 --http-password=f8c1508e03a43feaaedf507bc0e7d60a0c0f0485 https://digit-force-generic.pkg.coding.net/ai-platform/others"
RUN echo "machine digit-force-generic.pkg.coding.net" >> ~/.netrc \
    && echo "login others-1657101930760" >> ~/.netrc \
    && echo "password f8c1508e03a43feaaedf507bc0e7d60a0c0f0485" >> ~/.netrc
RUN sed -i s/archive.ubuntu.com/mirrors.aliyun.com/g /etc/apt/sources.list \
    && sed -i s/security.ubuntu.com/mirrors.aliyun.com/g /etc/apt/sources.list \
    && apt-get update \
    && apt-get install wget -y
RUN mkdir -p $SPARK_JARS && cd $SPARK_JARS \
    && $WGET_COMMAND/mysql-connector-java-5.1.44.jar


# 编译镜像
# docker build -t digit-force-docker.pkg.coding.net/ai-platform/base-images/spark-k8s-runtime-with-jars:3.2.1 -f spark321-k8s-runtime.Dockerfile .
# 上传镜像
# docker push digit-force-docker.pkg.coding.net/ai-platform/base-images/spark-k8s-runtime-with-jars:3.2.1