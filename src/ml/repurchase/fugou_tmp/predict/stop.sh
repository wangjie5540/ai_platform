#!/usr/bin/env bash

# root_dir="/home/ss_pujianyu/smart_people/file_chushu_api_test2"

echo "---------启动服务主程序脚本----"
#服务主程序脚本
main_scris1="check_run_fugou_predict.py"
main_scris2="Auto_predict_main.py"

# 获取该脚本进程
num2=`ps -ef | grep ${main_scris1} | grep -v grep | awk '{print $2}'`
# 打印该进程，若不存在该进程，则不进行任何操作，若有该脚本进程，则杀死这些进程
echo "main-scripts进程数=" ${num2}
if [ "$num2" == "" ]
then
    echo "主程序脚本无进程执行" 
else
    ps -ef | grep ${main_scris1} | grep -v grep | awk '{print $2}'| xargs kill -9
    echo "kill main-scripts1 success!"
fi

# 获取该脚本进程
num3=`ps -ef | grep ${main_scris2} | grep -v grep | awk '{print $2}'`
# 打印该进程，若不存在该进程，则不进行任何操作，若有该脚本进程，则杀死这些进程
echo "main-scripts进程数=" ${num3}
if [ "$num3" == "" ]
then
    echo "主程序脚本无进程执行" 
else
    ps -ef | grep ${main_scris2} | grep -v grep | awk '{print $2}'| xargs kill -9
    echo "kill main-scripts2 success!"
fi