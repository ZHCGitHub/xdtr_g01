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
java -jar CatchLink.jar /opt/xdtr_project/G01/getSensitiveWord/webSource/ ${number}

java -jar GetLinkSource.jar /opt/xdtr_project/G01/getSensitiveWord/filetmp/

#判断hdfs上是否存在目录test1，如果存在则删除test1文件夹里的内容
hadoop fs -test -e /xdtrdata/G01/data/test1
if [ $? -eq 0 ]
then
    hadoop fs -rm -r /xdtrdata/G01/data/test1/*
else
    hadoop fs -mkdir /xdtrdata/G01/data/test1
fi

hadoop fs -moveFromLocal /opt/xdtr_project/G01/getSensitiveWord/filetmp/* /xdtrdata/G01/data/test1/

rm -rf /opt/xdtr_project/G01/getSensitiveWord/webSource/*

#判断hdfs上是否存在目录test2，如果存在则删除
hadoop fs -test -e /xdtrdata/G01/data/test2
if [ $? -eq 0 ]
then
    hadoop fs -rm -r /xdtrdata/G01/data/test2
fi

sqoop import --connect jdbc:mysql://192.168.12.12:3306/G01 --username root --password 123456 --split-by id --table tbc_dic_url_link_fileName -m 1 --hive-drop-import-delims --fields-terminated-by "$" --target-dir /xdtrdata/G01/data/test2/;

hadoop fs -rm -r /xdtrdata/G01/data/test2/_SUCCESS

hadoop fs -mv /xdtrdata/G01/data/test2/part-m-00000 /xdtrdata/G01/data/test2/urlContent.txt

end_time=$(date '+%s')

run_time=$((${end_time} - ${start_time}));
echo "程序运行耗时"${run_time}"秒"









