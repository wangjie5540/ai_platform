# coding: utf-8
import os
import subprocess

tag = 'latest'

host_list = list()
with open('hosts', 'r', encoding='utf-8') as f:
    lines = f.readlines()
    for line in lines:
        line = line.strip()
        ip, host = line.strip().split()
        host_list.append(f'--add-host {host}:{ip}')

context = os.path.dirname(__file__)
cmd = f"docker build -f Dockerfile-jupyterlab -t jupyterlab:{tag} {' '.join(host_list)} {context}"
subprocess.check_call(cmd, shell=True)
