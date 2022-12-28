#!/bin/bash
import subprocess

res = subprocess.check_output("docker images | grep registry | awk '{print $1, $2}'", shell=True,
                              encoding='utf-8').strip().split('\n')
subprocess.call(
    "docker login -u third-1652703481311 -p 0edbd24f0c5d459c089907107a3918debf3537de digit-force-docker.pkg.coding.net",
    shell=True)
for line in res:
    image_full, tag = line.split(' ')
    image_name = image_full.split('/')[-1]
    tag_cmd = f'docker tag {image_full}:{tag} digit-force-docker.pkg.coding.net/ai-platform/third/{image_name}:{tag}'
    subprocess.check_call(tag_cmd, shell=True)
    push_cmd = f'docker push digit-force-docker.pkg.coding.net/ai-platform/third/{image_name}:{tag}'
    subprocess.check_call(push_cmd, shell=True)
