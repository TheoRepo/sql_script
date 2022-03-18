#!/bin/bash
source /etc/profile
source ~/.bash_profile

rss_path=$1
database_name=$2
source_table_name=$3
new_table_name=$4
pt=$5

sql_part="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${database_name}.${new_table_name} partition(the_date) select * from ${database_name}.${source_table_name} where the_date='${pt}' limit 10;
"

cd /home/${rss_path}/ && bash rss.sh "data_explorer" "nlp_dev" "$sql_part"

if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 数据写入完成