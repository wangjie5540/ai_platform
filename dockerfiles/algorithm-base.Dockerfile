FROM aip-tcr.tencentcloudcr.com/aip/python:3.7
ARG ROOT_DIR=/root
ARG ROOT_PASSWORD=123
WORKDIR $ROOT_DIR
ARG WGET_COMMAND="wget --http-user=others-1657101930760 --http-password=f8c1508e03a43feaaedf507bc0e7d60a0c0f0485 https://digit-force-generic.pkg.coding.net/ai-platform/others"
RUN echo "machine digit-force-generic.pkg.coding.net" >> ~/.netrc \
    && echo "login others-1657101930760" >> ~/.netrc \
    && echo "password f8c1508e03a43feaaedf507bc0e7d60a0c0f0485" >> ~/.netrc

# 添加用户
RUN echo "root:$ROOT_PASSWORD" | chpasswd && yum install -y openssh-server expect
# 传递清理jupyter notebook脚本
COPY clear_notebook_password.sh $ROOT_DIR
COPY start.sh $ROOT_DIR
# 添加jupyter
RUN pip install jupyter -i https://pypi.tuna.tsinghua.edu.cn/simple \
    && jupyter notebook --generate-config \
    && chmod +x $ROOT_DIR/clear_notebook_password.sh
# 添加jdk
RUN mkdir -p /opt && cd /opt && $WGET_COMMAND/jdk1.8.0_181-cloudera.zip && unzip jdk1.8.0_181-cloudera.zip
ENV JAVA_HOME=/opt/jdk1.8.0_181-cloudera
# 添加spark
RUN mkdir -p /opt && cd /opt && $WGET_COMMAND/spark-2.4.8-bin-hadoop2.7.tgz && tar xvf spark-2.4.8-bin-hadoop2.7.tgz
ENV SPARK_HOME=/opt/spark-2.4.8-bin-hadoop2.7
# 添加依赖包
ARG SPARK_JARS=/opt/spark-2.4.8-bin-hadoop2.7/jars
RUN cd $SPARK_JARS \
    && $WGET_COMMAND/mysql-connector-java-5.1.44.jar \
    && $WGET_COMMAND/common-1.0-SNAPSHOT.jar \
    && $WGET_COMMAND/starrocks-spark-writer-2.4_2.11-1.0-SNAPSHOT.jar \
    && $WGET_COMMAND/starrocks-spark2_2.11-1.0.0.jar \
    && $WGET_COMMAND/graphframes-0.8.2-spark2.4-s_2.11.jar
# 安装pyhive
RUN conda install --yes pyhive
# 安装依赖包
COPY requirements.txt $ROOT_DIR
RUN pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install torch==1.10.1+cpu torchvision==0.11.2+cpu torchaudio==0.10.1 -f https://download.pytorch.org/whl/torch_stable.html

# 编译镜像
# docker build -t aip-tcr.tencentcloudcr.com/aip/algorithm-base -f algorithm-base.Dockerfile .
# 上传镜像
# docker push aip-tcr.tencentcloudcr.com/aip/algorithm-base
# 启动容器
# docker run --pull always -p 2222:22 -p 9999:8888  -d --name algorithm-base --privileged=true --rm -it digit-force-docker.pkg.coding.net/ai-platform/base-images/algorithm-base /usr/sbin/init
# 开发阶段-启动ssh和jupyter-notebook
# docker exec -i algorithm-base sh start.sh