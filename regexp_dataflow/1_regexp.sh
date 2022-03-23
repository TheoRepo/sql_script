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
msg regexp '车' OR
app_name regexp '车' OR
suspected_app_name regexp '车'
) AND
(
msg not regexp '([约专快打租单托拖运拼风叫机停泊有用行客货派])车' AND
app_name not regexp '([约专快打租单托拖运拼风叫机停泊有用行客货派])车' AND
suspected_app_name not regexp '([约专快打租单托拖运拼风叫机停泊有用行客货派])车'
) AND
(
msg not regexp 污染天气.{1,4}预警|车费未支付|投保|保险|续保|保费|车险|流量直通车|预警|公交|基金|已退款成功|消费金融|验证码|点击领取|(?<=参加“).*?(?=”活动)|安全|侧滑甩尾|回[a-zA-Z]退|[a-zA-Z]{2}退订|回复[a-zA-Z]{2}退订|取消预约|文明|车主提醒|您[的有].{2,6}包裹|快递|核酸|ETC|贷款|车贷|罚款|扣款交易|违法行为|挪车|如实申报|优享型司机|接单条件|卸\d{1,10}吨|增\d{1,10}吨|抢单|口罩|等待救援|取件码|车门未上锁|检票口|物流|货运|退订|金融|交通违法|社区居民委员会|行为规范|尽快还车|计费|智能盒|液化气|石化|工业|装车费|视频|提单号|防疫物资|支付乘车费用|[公|中|吊|拖|卡|重|拖|叉|用]车|装卸车|升降车|旅行车|公务车|助力车|自行车|共享.*车 AND
app_name not regexp 手机报|快报|保险|流量直通车|公交|基金|金融|ETC|资讯|车管所|铁路|视频 AND
suspected_app_name not regexp 手机报|快报|保险|流量直通车|公交|基金|金融|ETC|资讯|车管所|铁路|视频
)
;
"

cd /home/${rss_path}/ && bash rss.sh "data_explorer" "nlp_dev" "$sql_part"

if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 数据写入完成

