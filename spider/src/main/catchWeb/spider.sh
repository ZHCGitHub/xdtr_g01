#! /bin/bash

#--------------------------------------------
# 这是一个从101.201.55.199上下载数据的shell脚本
#
# 功能：从mysql数据库中导出为txt文件
# 特色：
#--------------------------------------------
##########################################################
#######运行程序
##########################################################
number=$1
start_time=$(date '+%s')

ip="192.168.12.125"
database="gov01_v3"
user="root"
passworld="123"

mysql -u${user} -p${passworld} -h${ip} -e "
use ${database}
##################清空表tbc_dic_web_title_tmp
DELETE FROM tbc_dic_site_link;
DELETE FROM tbc_dic_site_link_filename;
"

java -jar CatchAllLink.jar /opt/xdtr_project/G01/getSensitiveWord/webSource/ 3

java -jar GetLinkSource.jar /opt/xdtr_project/G01/getSensitiveWord/filetmp/

#判断hdfs上是否存在目录swData，如果存在则删除swData文件夹里的内容
#hadoop fs -test -e /xdtrdata/G01/data/swData
#if [ $? -eq 0 ]
#then
#    hadoop fs -rm -r /xdtrdata/G01/data/swData/*
#else
#    hadoop fs -mkdir /xdtrdata/G01/data/swData
#fi

hadoop fs -moveFromLocal /opt/xdtr_project/G01/getSensitiveWord/filetmp/* /xdtrdata/G01/data/swData/
#hadoop dfs -put /home/test/swData /xdtrdata/G01/data/swData

##rm -rf /opt/xdtr_project/G01/getSensitiveWord/webSource/*
#
##判断hdfs上是否存在目录spareWord，如果存在则删除
#hadoop fs -test -e /xdtrdata/G01/data/spareWord
#if [ $? -eq 0 ]
#then
#    hadoop fs -rm -r /xdtrdata/G01/data/spareWord
#fi
#
#sqoop import --connect jdbc:mysql://${ip}:3306/${database} --username ${user} --password ${passworld} --split-by id --table tbc_dic_site_link_filename -m 1 --hive-drop-import-delims --fields-terminated-by "$" --target-dir /xdtrdata/G01/data/spareWord/
#
#hadoop fs -rm -r /xdtrdata/G01/data/spareWord/_SUCCESS
#
#hadoop fs -mv /xdtrdata/G01/data/spareWord/part-m-00000 /xdtrdata/G01/data/spareWord/urlContent.txt
#
#end_time=$(date '+%s')
#
#run_time=$((${end_time} - ${start_time}));

echo "程序运行耗时"${run_time}"秒"