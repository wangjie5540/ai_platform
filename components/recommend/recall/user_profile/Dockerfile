FROM pytorch/pytorch:1.9.0-cuda11.1-cudnn8-runtime

ARG PROKECT_DIR=/app/ss-kubeflow-pipeline-component

RUN mkdir -p $PROKECT_DIR

WORKDIR $PROKECT_DIR
ENV PYTHONPATH=$PROKECT_DIR

COPY requirements.txt $PROKECT_DIR/requirements.txt
RUN pip install --ignore-installed --use-deprecated=legacy-resolver -r requirements.txt  -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY ./common $PROKECT_DIR
COPY ./recommend/recall/user_profile/user_profile_recall.py $PROKECT_DIR/main.py