FROM digit-force-docker.pkg.coding.net/ai-platform/base-images/miniconda3-base:latest
ARG COMPONENT_DIR=/componet
CMD mkdir -p $COMPONENT_DIR
WORKDIR $COMPONENT_DIR
COPY common ./common
COPY components/ml/uplift .
CMD ["python", "-V"]