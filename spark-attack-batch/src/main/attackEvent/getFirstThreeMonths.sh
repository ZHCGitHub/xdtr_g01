#! /bin/bash
#--------------------------------------------
# 这是运行攻击事件批处理jar包的脚本
#
# 功能：处理2017-07-20号开始到2017-05-01的数据
# 特色：
#--------------------------------------------
##### 用户配置区 开始 #####
DATE="2017-07-20"
function getDaysBefore()
{
 sec=`date -d $1 +%s`
 sec_DaysBefore=$((sec - 86400*1))
 days_before=`date -d @$sec_DaysBefore +%F`
 echo ${days_before}
}

for((i=1;i<=81;i++));
do
echo ${DATE}
spark-submit --master yarn-client --num-executors 6 --executor-memory 5G --executor-cores 5 --driver-memory 10G  --class AttackEventBatch  /opt/xdtr_project/G01/attackEvent/AttackEventBatch.jar ${DATE}
DATE=`getDaysBefore ${DATE}`
done

