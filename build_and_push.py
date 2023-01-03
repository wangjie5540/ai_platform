# coding=utf-8
import logging
import os


def get_dockerfile_content(image_dir, bottom_image_name=None):
    if bottom_image_name is None:
        bottom_image_name = "digit-force-docker.pkg.coding.net/ai-platform/base-images/miniconda3-base:latest"
    return """FROM {0}

ARG PROJECT_DIR=/app/digit-force-kubeflow-pipeline-component-image

RUN mkdir -p $PROJECT_DIR

WORKDIR $PROJECT_DIR
ENV PYTHONPATH=$PROJECT_DIR

RUN mkdir -p $PROJECT_DIR/digitforce/aip
COPY ./digitforce/__init__.py $PROJECT_DIR/digitforce/__init__.py
COPY ./digitforce/aip/__init__.py $PROJECT_DIR/digitforce/aip/__init__.py

COPY ./digitforce/aip/common $PROJECT_DIR/digitforce/aip/common
COPY ./digitforce/aip/components $PROJECT_DIR/digitforce/aip/components

COPY {1}/*py $PROJECT_DIR/""".format(bottom_image_name, image_dir)


def generate_docker_file(one_dir, bottom_image_name=None, tag="latest"):
    dockerfile_path = os.path.join(one_dir, "Dockerfile")
    bottom_image_name_path = os.path.join(one_dir, "start_image")
    if os.path.exists(bottom_image_name_path):
        with open(bottom_image_name_path) as fi:
            for _ in fi:
                if _:
                    bottom_image_name = _.strip()

    if not os.path.exists(dockerfile_path):
        with open(dockerfile_path, mode='w', encoding='utf-8') as fo:
            fo.write(get_dockerfile_content(one_dir, bottom_image_name))

    image_name = "digit-force-docker.pkg.coding.net/ai-platform/ai-components" \
                 "/{0}:{1}".format(one_dir.replace('/', '-'), tag)
    build_cmd = "docker build -t " \
                "{0}" \
                " -f {1} .".format(image_name, dockerfile_path)
    os.system(build_cmd)
    push_cmt = "docker push {0}".format(image_name)
    os.system(push_cmt)


def find_main_file(one_dir, result):
    if not os.path.exists(one_dir):
        logging.warning("this dir is not exists dir:" + str(one_dir))
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
    os.system("docker login -u ai-components-1672712149820 -p 30dd16ad7d172c138cdc4475133ba6d67b8fae09 digit-force-docker.pkg.coding.net")
    for _dir in [
        "src/feature_engineering",
        "src/sample",

    ]:
        result = []
        find_main_file(_dir, result)
        for _ in result:
            generate_docker_file(_)


if __name__ == '__main__':
    main()
