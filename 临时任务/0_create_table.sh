#!/bin/bash
source /etc/profile
source ~/.bash_profile

sql_part="
drop table if exists mkt_source.zt_qianyu_20220411;
create table if not exists mkt_source.zt_qianyu_20220411(
row_key string COMMENT '行key',     
mobile_id string COMMENT '被叫',     
event_time string COMMENT '事件时间',     
app_name string COMMENT '明确签名',    
suspected_app_name string COMMENT '疑似签名',
msg string COMMENT '文本',     
main_call_no string COMMENT '主叫',    
abnormal_label string COMMENT '异常标识',  
rule_id string COMMENT '命中规则ID',    
hashcode string COMMENT 'simhash标识',
file_no string COMMENT 'xxxx_YYYYMMDD_xxxx'
)
PARTITIONED BY (                  
the_date string COMMENT 'YYYY-MM-DD 分区1', 
label string COMMENT '标签编号 分区2'
)
;
"

beeline -u "jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "$sql_part"
if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 建表完成


