FROM continuumio/miniconda3:4.10.3

ARG PROKECT_DIR=/app/ss-kubeflow-pipeline-component

RUN mkdir -p $PROKECT_DIR

WORKDIR $PROKECT_DIR
ENV PYTHONPATH=$PROKECT_DIR

CMD pip  --default-timeout=1000  install --ignore-installed --use-deprecated=legacy-resolver -r requirements.txt

COPY ./common $PROKECT_DIR
COPY ./preprocess/dataset/mf/data_generator.py $PROKECT_DIR/main.py