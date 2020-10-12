import re
import logging

pattern = [
    u"%Y-%m-%d",
    u"%Y-%m-%d %H:%M",
    u"%Y-%m-%d  %H:%M",
    u"%Y-%m-%d %H:%M:%S",
    u"%Y-%m-%d  %H:%M:%S",
    u"%Y/%m/%d",
    u"%Y/%m/%d %H:%M",
    u"%Y/%m/%d  %H:%M",
    u"%Y/%m/%d %H:%M:%S",
    u"%Y/%m/%d  %H:%M:%S",
    u"%Y %m/%d",
    u"%Y %m/%d %H:%M",
    u"%Y %m/%d  %H:%M",
    u"%Y %m/%d %H:%M:%S",
    u"%Y %m/%d  %H:%M:%S",
    u"%Y年%m月%d日",
    u"%Y年%m月%d日 %H时%M分",
    u"%Y年%m月%d日  %H时%M分",
    u"%Y年%m月%d日 %H时%M分%S秒",
    u"%Y年%m月%d日  %H时%M分%S秒"
]

def get_date(str):
    for p in pattern:
        try:
            date = time.strptime(str, p)
            if date:
                return date
        except:
            continue
    return None



logging.basicConfig(level=logging.INFO)

removes = ["发布机构：", "文章来源：","信息来源：", "来源：", "时间：", "《", "》","发布机构:","发文机关:","【","】","发文机构 ：","发文单位：","发文机关："]
splits = ["/", " ", "、", "||", "－"]

# 删除removes中包含关键字
def remove_normalize(line):
    result = line
    for r in removes:
        if result.find(r) != -1:
            result = result.strip(r)
    return result.strip()

# 按照splits包含关键字进行切分
def split_normalize(line):
    result = line
    for s in splits:
        if result.find(s) != -1:
            return True, result.split(s)
    return False, [result.strip()]

# 查找规则匹配
def rule_normalize(line, rule):
    result = line
    for regex in rule:
        matcher = re.search(regex, result, re.I)
        if matcher:
            result = matcher.group(0)
    return result

# 检测是否包含数字
def has_digit(processor):
    return re.search(r'\d+', processor);

# 中文组织信息归一化
def normalize_zh(line):
    pre_remove = remove_normalize(line.strip().replace("\xa0", " "))
    pre_processor = pre_remove
    
    dd = pre_processor
    if dd.find("来源:") != -1:
        matcher = re.search(r"来源:([\s]*[\u4e00-\u9fa5]*)", dd, re.I)
        if matcher:
            dd = matcher.group(1)
    pre_processor = dd

    if has_digit(pre_processor):
        if pre_processor.find("来源：") != -1:
            pre_filter = pre_remove.split("来源：")
            filter = []
            for f in pre_filter:
                if len(f.strip()) != 0 and (not has_digit(f)):
                    filter.append(f)
            if len(filter) != 0:
                pre_processor = filter[0]
        elif pre_processor.find("更新时间：") != -1:
            pre_filter = pre_remove.split("更新时间：")
            filter = []
            for f in pre_filter:
                if len(f.strip()) != 0 and (not has_digit(f)):
                    filter.append(f)
            if len(filter) != 0:
                pre_processor = filter[0]
            else:
                post_filter = []
                for f in pre_filter:
                    if len(f.strip()) != 0 and (not get_date(f)):
                        post_filter.append(f)
                if len(post_filter) != 0:
                    pre_processor = post_filter[0]
        else:
            pre_rule = [r'[\u4e00-\u9fa5]+']
            pre_processor = rule_normalize(pre_remove, pre_rule)

    label, split = split_normalize(pre_processor.strip())
    split_wrap = []
    if label:
        # 执行分割操作
        split_wrap.extend(split)
    else:
        # 数据没有切分
        split_wrap.append(split[0])

    # 后置处理器
    post_rule = []
    result = []
    for s in split_wrap:
        result.append(rule_normalize(s, post_rule))
    return result

# 英文组织信息归一化
def normalize_en(line):
    pass

def normalize(line, options):
    line = line.replace("++__))((","")
    if isinstance(line, str):
        if options == "en":
            return normalize_en(line)
        else:
            return normalize_zh(line)
    return [line]

if __name__ == '__main__':
    lines = [
        "来源：中国人大网",
        "对外贸易经济合作部、国家外汇管理局",
        "《时事报告》",
        "发布机构：人民日报",
        "文章来源：人民日报",
        "教育部、国务院扶贫办、国家语委",
        "教育部 国家语委",
        "来源：中共云南省委政策研究室 2020-05-20 11:45:00",
        "2020-05-20 11:45:00 来源：中共云南省委政策研究室",
        "2020年02月14日    来源：指导管理司",
        "国家外汇管理局 海关总署 国家税务总局",
        "文章来源：办公室    更新时间：2015-04-29 12:17",
        "时间：2018年05月11日 18:30 来源：退役军人事务部",
        "2017年09月21日    来源：政策法规司",
        "文章来源：2016年第3号    更新时间：2016-07-05 16:52",
        "2020年07月02日    来源：政策法规司",
        "2017年01月19日\xa0\xa0\xa0\xa0来源：政策法规司",
        "信息来源：管理员  被阅览数：155次  发布人：安徽人大网   发布日期：2003-08-27",

        "来源：节能与综合利用司",
        "2020年2月8日 来源：金融司",
        "时间：2020年01月14日 16:01 来源：拥军优抚司",
        "信息来源：备案审查处\xa0\xa0被阅览数：2203次\xa0\xa0发布人：安徽人大网 \xa0\xa0发布日期：2014-11-21",
        "文章来源：办公室    更新时间：2017-11-01 16:07",
        "发布时间：2020年07月20日\xa0\xa0信息来源：省商务厅",
        "办公厅信息中心 　  2019-01-24",
        "办公厅信息中心 \u3000  2019-03-19",
        "市场监管总局 发展改革委 财政部 人力资源社会保障部 商务部 人民银行",
        # "机关党委、人事司",
        "【时间: 】    【来源:审计署】字号：",
        "信息来源：管理员  被阅览数：155次  发布人：安徽人大网   发布日期：2003-08-27",
        "【时间: 】    【来源:中国内部审计协会 审计署内部审计指导监督司】字号：",
        "来源:  办公室",
        "发文机关：河北省人民政府",
        "发文机关：国家教委、国家体委"
    ]
    for line in lines:
        logging.info(normalize(line, "zh"))
