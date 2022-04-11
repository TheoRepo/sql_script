#!/bin/bash
source /etc/profile
source ~/.bash_profile

rss_path=$1

sql_part="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table mkt_source.zt_qianyu_20220411 partition(the_date, label)
select row_key, mobile_id, event_time, app_name, suspected_app_name, msg, main_call_no, abnormal_label, rule_id, hashcode, file_no, the_date, '云001' as label
from preprocess.ds_txt_final_sample where 
the_date regexp '2021-12-(02|03|04)' and
(app_name regexp '百度智能云' or suspected_app_name regexp '百度智能云') and
msg regexp '验证|实名认证' and
main_call_no regexp '106940502800790001'
;

insert overwrite table mkt_source.zt_qianyu_20220411 partition(the_date, label)
select row_key, mobile_id, event_time, app_name, suspected_app_name, msg, main_call_no, abnormal_label, rule_id, hashcode, file_no, the_date, '云002' as label
from preprocess.ds_txt_final_sample where 
the_date regexp '2021-12-(02|03|04)' and
(app_name regexp '火山引擎' or suspected_app_name regexp '火山引擎') and
msg regexp '注册|实名认证|购买|开通|服务' and
main_call_no regexp '10692305670950358250|10680117000332'
;

insert overwrite table mkt_source.zt_qianyu_20220411 partition(the_date, label)
select row_key, mobile_id, event_time, app_name, suspected_app_name, msg, main_call_no, abnormal_label, rule_id, hashcode, file_no, the_date, '云003' as label
from preprocess.ds_txt_final_sample where 
the_date regexp '2021-12-(02|03|04)' and
(app_name regexp '金山云' or suspected_app_name regexp '金山云') and
msg regexp '注册|企业实名认证|对公账户' and
main_call_no regexp '10691216489200311'
;

insert overwrite table mkt_source.zt_qianyu_20220411 partition(the_date, label)
select row_key, mobile_id, event_time, app_name, suspected_app_name, msg, main_call_no, abnormal_label, rule_id, hashcode, file_no, the_date, '云004' as label
from preprocess.ds_txt_final_sample where 
the_date regexp '2021-12-(02|03|04)' and
(app_name regexp '腾讯云' or suspected_app_name regexp '腾讯云') and
msg regexp '注册|实名认证|企业|企业对公打款|认证成功' and
main_call_no regexp '1069236600000020|10681546930920|1063060888900102033|1069103266660|1069121670101000'
;

insert overwrite table mkt_source.zt_qianyu_20220411 partition(the_date, label)
select row_key, mobile_id, event_time, app_name, suspected_app_name, msg, main_call_no, abnormal_label, rule_id, hashcode, file_no, the_date, '云005' as label
from preprocess.ds_txt_final_sample where 
the_date regexp '2021-12-(02|03|04)' and
(app_name regexp 'UCloud' or suspected_app_name regexp 'UCloud') and
msg regexp '绑定|完成验证|实名认证'
;

insert overwrite table mkt_source.zt_qianyu_20220411 partition(the_date, label)
select row_key, mobile_id, event_time, app_name, suspected_app_name, msg, main_call_no, abnormal_label, rule_id, hashcode, file_no, the_date, '云006' as label
from preprocess.ds_txt_final_sample where 
the_date regexp '2021-12-(02|03|04)' and
(app_name regexp '京东|京东云|京东科技' or suspected_app_name regexp '京东|京东云|京东科技') and
msg regexp '京东注册|完成认证|企业上云' and
main_call_no regexp '10659205760001125886|10691274566399'
;

insert overwrite table mkt_source.zt_qianyu_20220411 partition(the_date, label)
select row_key, mobile_id, event_time, app_name, suspected_app_name, msg, main_call_no, abnormal_label, rule_id, hashcode, file_no, the_date, '云007' as label
from preprocess.ds_txt_final_sample where 
the_date regexp '2021-12-(02|03|04)' and
(app_name regexp '西部数码' or suspected_app_name regexp '西部数码') and
msg regexp '注册|登录'
;

insert overwrite table mkt_source.zt_qianyu_20220411 partition(the_date, label)
select row_key, mobile_id, event_time, app_name, suspected_app_name, msg, main_call_no, abnormal_label, rule_id, hashcode, file_no, the_date, '云008' as label
from preprocess.ds_txt_final_sample where 
the_date regexp '2021-12-(02|03|04)' and
(app_name regexp '阿里云' or suspected_app_name regexp '阿里云') and
msg regexp '校验码|注册' and
main_call_no regexp '10691996500000458576|10693695700000458576|10630618000000458519|10659025620000458748';
;
"

cd /home/${rss_path}/ && bash rss.sh "temp_task" "nlp_dev" "$sql_part"

if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 数据写入完成