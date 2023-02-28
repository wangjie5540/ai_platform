#!/bin/bash

echo 'export SPARK_HOME=/opt/spark-2.4.8-bin-hadoop2.7' >> /root/.bashrc
source activate
conda activate python36
/root/clear_notebook_password.sh
systemctl start sshd
jupyter notebook --no-browser --allow-root --ip=0.0.0.0 --port 8888 --ServerApp.allow_origin='*' > /dev/null &
