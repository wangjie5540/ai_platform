FROM digit-force-docker.pkg.coding.net/ai-platform/base-images/algorithm-base as miniconda
# spark-k8s-runtime:2.4.8镜像是通过spark-2.4.8-bin-hadoop2.7打包获得，命令如下：
# docker build -t aip-tcr.tencentcloudcr.com/aip/spark-k8s-runtime:2.4.8 -f kubernetes/dockerfiles/spark/Dockerfile .
FROM aip-tcr.tencentcloudcr.com/aip/spark-k8s-runtime:2.4.8
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
    && $WGET_COMMAND/mysql-connector-java-5.1.44.jar \
    && $WGET_COMMAND/common-1.0-SNAPSHOT.jar \
    && $WGET_COMMAND/starrocks-spark-writer-2.4_2.11-1.0-SNAPSHOT.jar \
    && $WGET_COMMAND/starrocks-spark2_2.11-1.0.0.jar \
    && $WGET_COMMAND/alluxio-2.7.4-client.jar \
    && $WGET_COMMAND/graphframes-0.8.2-spark2.4-s_2.11.jar

COPY --from=miniconda /opt/miniconda3 /opt/miniconda3
COPY --from=miniconda /opt/spark-2.4.8-bin-hadoop2.7/python/lib ${SPARK_HOME}/python/lib
ENV PYTHONPATH ${SPARK_HOME}/python/lib/pyspark.zip:${SPARK_HOME}/python/lib/py4j-*.zip

# 构建镜像
# docker build -t aip-tcr.tencentcloudcr.com/aip/spark-k8s-runtime-with-jars_248:$TAG -f spark248-k8s-runtime.Dockerfile .
# docker push aip-tcr.tencentcloudcr.com/aip/spark-k8s-runtime-with-jars_248:$TAG