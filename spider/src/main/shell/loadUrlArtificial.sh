#! /bin/bash
#--------------------------------------------
# 这是一个从101.201.55.199上下载数据的shell脚本
#
# 功能：从mysql数据库中导出为txt文件
# 特色：
#--------------------------------------------
##########################################################
#######读取配置文件
##########################################################
#shell脚本当前目录
shell_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#conf目录的路径
conf=${shell_dir%/*}/conf
#读取配置文件
source ${conf}/g01-shell.cfg

##########################################################
######程序运行
##########################################################
#运行mysql语句
mysql -u${mysql_local_user} -p${mysql_local_passworld} -h${mysql_local_ip} -e "
use ${mysql_local_database}
##################清空表tbc_dic_web_title_tmp
DELETE FROM tbc_dic_web_title_tmp;
"

#切分省、市、县
java -jar /opt/xdtr_project/G01/getTitle/splitTitle.jar artificial









