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
app_name regexp '(?<![约专快打租单托拖运拼风叫机停泊有用行客货派])车$' OR
suspected_app_name regexp '(?<![约专快打租单托拼风叫机停泊有用行客货派])车$'
) AND
(
msg not regexp '移动彩印提醒|温馨提醒|共抗疫情|来自\d*|车险|交强险|投保|商业险|保险|强险|保费|车损|三者|首保|车船税|车船税|交强|财险|续保|机动车损失险|责任险|事故损失车辆|定损|理赔|报价单|报价|天气|[买购]房|房屋按揭|贷款装修|信用|征信|结婚|留学|进修|教育|旅游|飞机|电动车|摩托车|公务用车|卡车|叉车|大卡|吊车|升降车|物流车|验证码|登陆凭证|诈骗|动态密码|开通亲情网|司机|叫车|快车|专车|出租|顺风车|拼车|车票|雅迪|老板|车商|线索|待回访客户|直通车|待跟进意向|及时联系客户|餐厅|车间|员工|服务请求|警情号|公车|用车人|柴油|退订|回[a-zA-Z]退|回[a-zA-Z][a-zA-Z]退|拒收回[a-zA-Z]|点击立即领取|点击链接|诚挚来电|恭喜您|瓜分|积分奖励|推荐|详情|违章|违规|违法|交警|超速|限速|乘用车|发货|租赁|儿童车|证券|获利|短线|冲高|投资|止盈点|基金|大盘|美股|港股|指数|理财|珍爱网|红娘|已购车房|购车有房|购车购房|男士情况|男士条件|金联创|路况信息|车费|合伙人|好友申请|公安厅|绑定|登录|注册|官方旗舰店|小灵狗出行|车位|[0-9]*股|[0-9]*吨|银行客户|贷款逾期案件|失信执行人名单|黑名单|业主|维修|放假|面试|入职申请|学校|淘宝|客服|师傅|接单|中信建投|连信|快手科技|告警|节点|模块|CPU|接口|测试|集群|孩子|校门口|老师|家长|考试|借款申请|激活码' OR
app_name not regexp '哈罗|哈啰|滴答|滴滴|流量|共享.*车|自行车|助力车|公务用车|公务车|旅行车|外卖|公车|出警|中车|卡车|叉车|大卡|吊车|升降车|证券|基金|法院|法律|信用卡|公安[厅局]|银行|资讯|重汽|电商|汽车金融' OR
suspected_app_name not regexp '哈罗|哈啰|滴答|滴滴|流量|共享.*车|自行车|助力车|公务用车|公务车|旅行车|外卖|公车|出警|中车|卡车|叉车|大卡|吊车|升降车|证券|基金|法院|法律|信用卡|公安[厅局]|银行|资讯|重汽|电商|汽车金融'
)
;
"

cd /home/${rss_path}/ && bash rss.sh "data_explorer" "nlp_dev" "$sql_part"

if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 数据写入完成

