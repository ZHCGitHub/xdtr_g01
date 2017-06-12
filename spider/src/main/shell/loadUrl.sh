#! /bin/bash
#--------------------------------------------
# 这是一个爬取title，切分省、市、县的脚本
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
######路径配置
##########################################################
##########################################################
######程序运行
##########################################################
#运行mysql语句
mysql -u${mysql_local_user} -p${mysql_local_passworld} -h${mysql_local_ip} -e "
use ${mysql_local_database}
##################清空表tbc_dic_web_title_tmp
DELETE FROM tbc_dic_url_title;
DELETE FROM tbc_dic_web_title_tmp;
"


java -jar /opt/xdtr_project/G01/getTitle/getTitle.jar
java -jar /opt/xdtr_project/G01/getTitle/splitTitle.jar spider
#######################################################












