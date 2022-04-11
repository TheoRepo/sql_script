-- 看车询价
select msg from nlp_dev.qianyu_20220318_car where msg regexp '看车(?!辆定位)';
select msg from nlp_dev.qianyu_20220318_car where msg regexp '询价';

-- 买车
select msg from nlp_dev.qianyu_20220318_car where msg regexp '购得爱车';
select msg from nlp_dev.qianyu_20220318_car where msg regexp '竞价';

-- 摇号竞价
select msg from nlp_dev.qianyu_20220318_car where msg regexp '指标' and msg regexp '配置';
select msg from nlp_dev.qianyu_20220318_car where msg regexp '竞价';

--学车考试
select msg from nlp_dev.qianyu_20220318_car where msg regexp '学车';
select msg from nlp_dev.qianyu_20220318_car where msg regexp '约考';

--车牌号预选
select msg from nlp_dev.qianyu_20220318_car where msg regexp '预选号牌';

--车贷
select msg from nlp_dev.qianyu_20220318_car where msg regexp '车贷';

--记分办证
select msg from nlp_dev.qianyu_20220318_car where msg regexp '驾驶证' and msg regexp '计分';
select msg from nlp_dev.qianyu_20220318_car where msg regexp '驾驶证' and msg regexp '换证';

--备案
select msg from nlp_dev.qianyu_20220318_car where msg regexp '备案';

--年检年审
select msg from nlp_dev.qianyu_20220318_car where msg regexp '年检';

--洗车
select msg from nlp_dev.qianyu_20220318_car where msg regexp '洗车';

--保养、维修
select msg from nlp_dev.qianyu_20220318_car where msg regexp '保养';
select msg from nlp_dev.qianyu_20220318_car where msg regexp '维修';

--加油充值
select msg from nlp_dev.qianyu_20220318_car where msg regexp '加油' and msg regexp '元';

