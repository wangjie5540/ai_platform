#!/usr/bin/env bash


# 线上路径
# root_dir="/home/ss_pujianyu/smart_people/fugou"

echo "---------启动服务主程序脚本----"
#服务主程序脚本
main_scris1="check_run_fugou_train.py"
main_scris2="Auto_train_main.py"

# 日志文件
log_file1="check_run_fugou.log"
log_file2="train_nohup_output.log"
# 启动文件
nohup /data/anaconda3/envs/pjy-pyspark3.6/bin/python ${main_scris1} > ${log_file1} 2>&1 &
echo 'start 1 success!'
nohup /data/anaconda3/envs/pjy-pyspark3.6/bin/python ${main_scris2} > ${log_file2} 2>&1 &
echo 'start 2 success!'

echo ' over '
