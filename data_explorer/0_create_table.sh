#!/bin/bash
source /etc/profile
source ~/.bash_profile

database_name=$1
source_table_name=$2
new_table_name=$3

sql_part="
drop table if exists ${database_name}.${new_table_name};
create table ${database_name}.${new_table_name} like ${database_name}.${source_table_name};
alter table ${database_name}.${new_table_name} set fileformat textfile;
alter table ${database_name}.${new_table_name} set serdeproperties('serialization.format'='\t', 'field.delim'='\t');
"

beeline -u "jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "$sql_part"
if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 建表完成