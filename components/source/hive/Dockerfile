FROM shus1015yc-docker.pkg.coding.net/marketing_algorithm/ai-platform/spark-client-cdh:v0.1

ARG PROKECT_DIR=/app/ss-kubeflow-pipeline-component

RUN mkdir -p $PROKECT_DIR

WORKDIR $PROKECT_DIR
ENV PYTHONPATH=$PROKECT_DIR

COPY requirements.txt $PROKECT_DIR/requirements.txt
RUN pip  install --ignore-installed --use-deprecated=legacy-resolver -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY ./common $PROKECT_DIR/common
COPY ./io/hive/kv_to_table.py $PROKECT_DIR/main.py