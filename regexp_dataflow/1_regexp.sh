#!/bin/bash
source /etc/profile
source ~/.bash_profile

rss_path=$1
source_database_name=$2
source_table_name=$3
new_database_name=$4
new_table_name=$5

sql_part="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${new_database_name}.${new_table_name} partition(the_date,file_no) select * from ${source_database_name}.${source_table_name} where 
abnormal_label regexp '正常文本' AND
(
msg regexp '(?<![约专快打租单托拖运拼风叫机停泊有用行客货派])车' OR
app_name regexp '(?<![约专快打租单托拖运拼风叫机停泊有用行客货派])车$' OR
suspected_app_name regexp '(?<![约专快打租单托拖运拼风叫机停泊有用行客货派])车$'
) 
;
"

cd /home/${rss_path}/ && bash rss.sh "data_explorer" "nlp_dev" "$sql_part"

if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 数据写入完成

