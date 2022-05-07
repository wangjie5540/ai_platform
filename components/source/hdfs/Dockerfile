FROM continuumio/miniconda3:4.10.3

ARG PROKECT_DIR=/app/ss-kubeflow-pipeline-component

RUN mkdir -p $PROKECT_DIR

WORKDIR $PROKECT_DIR
ENV PYTHONPATH=$PROKECT_DIR

COPY requirements.txt $PROKECT_DIR/requirements.txt
RUN pip  install --ignore-installed --use-deprecated=legacy-resolver -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY ./common $PROKECT_DIR/common
COPY ./io/hdfs/hdfs_to_local.py $PROKECT_DIR/main.py