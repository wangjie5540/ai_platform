# coding=utf-8
import logging
import os

from digitforce.aip.common.logging_config import setup_console_log


def get_dockerfile_content(image_dir, bottom_image_name=None):
    if bottom_image_name is None:
        bottom_image_name = "digit-force-docker.pkg.coding.net/ai-platform/base-images/miniconda3-base:latest"
    return f'''FROM {bottom_image_name}

ARG PROJECT_DIR=/app/digit-force-kubeflow-pipeline-component-image

RUN mkdir -p $PROJECT_DIR

WORKDIR $PROJECT_DIR
ENV PYTHONPATH=$PROJECT_DIR

RUN mkdir -p $PROJECT_DIR/digitforce/aip
COPY ./digitforce/__init__.py $PROJECT_DIR/digitforce/__init__.py
COPY ./digitforce/aip/__init__.py $PROJECT_DIR/digitforce/aip/__init__.py

COPY ./digitforce/aip/common $PROJECT_DIR/digitforce/aip/common
COPY ./digitforce/aip/cgf $PROJECT_DIR/digitforce/aip/cgf

COPY {image_dir}/*py $PROJECT_DIR/

'''


def generate_docker_file(one_dir, bottom_image_name=None, tag="latest"):
    dockerfile_path = os.path.join(one_dir, "Dockerfile")
    bottom_image_name_path = os.path.join(one_dir, "start_image")
    if os.path.exists(bottom_image_name_path):
        with open(bottom_image_name_path) as fi:
            for _ in fi:
                if _:
                    bottom_image_name = _.strip()

    with open(dockerfile_path, mode='w', encoding='utf-8') as fo:
        fo.write(get_dockerfile_content(one_dir, bottom_image_name))

    image_name = f"digit-force-docker.pkg.coding.net/ai-platform/ai-components" \
                 f"/{one_dir.replace('/', '-')}:{tag}"
    build_cmd = f"docker build -t " \
                f"{image_name}" \
                f" -f {dockerfile_path} ."
    os.system(build_cmd)
    push_cmt = f"docker push " \
               f"{image_name}"
    os.system(push_cmt)


def find_main_file(one_dir, result):
    if not os.path.exists(one_dir):
        logging.warning(f"this dir is not exists dir:{one_dir}")
    if os.path.isdir(one_dir):
        if os.path.exists(os.path.join(one_dir, "main.py")):
            result.append(one_dir)
            return result
        child_dirs = os.listdir(one_dir)
        for _dir in child_dirs:
            _dir = os.path.join(one_dir, _dir)
            if os.path.isdir(_dir):
                find_main_file(_dir, result)


def main():
    setup_console_log()
    for _dir in [
        "src/recommend/recall/recall_result_to_redis",
        # "src/recommend/recall/mf",
        # "src/recommend/recall/similarity_search",
        # "src/recommend/recall",
        # "src/data_preprocess",
        # "src/source",
        # "src/test",
        # "src/deeplearning",
    ]:
        result = []
        find_main_file(_dir, result)
        for _ in result:
            generate_docker_file(_)


if __name__ == '__main__':
    main()
