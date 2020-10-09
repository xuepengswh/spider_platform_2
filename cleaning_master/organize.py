import re
import logging
import time

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

def get_str(date):
    return time.strftime("%Y-%m-%d", date)

logging.basicConfig(level=logging.INFO)

removes = ["发布机构：", "发布机构:","文章来源：","文章来源:", "更新时间：","更新时间:","来源：","来源:", "时间：", "时间:","《", "》","发文机关：","发文机关:"]
splits = ["/", " ", "、", "||", "－"]

# 删除removes中包含关键字
def remove_normalize(line):
    result = line
    # result = line.replace(" ","")
    for r in removes:
        if has_digit(line):
            if r=="来源：" or r=="更新时间：":  #字符包含数字并且这两个字符这种情况在后面会处理
                continue
        if result.find(r) != -1:
            result = result.replace(r," ")
    # print(result)
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
    # pre_processor = line
    # pre_remove = line
    if has_digit(pre_processor):
        if pre_processor.find("来源：") != -1:  #字符包含来源
            if pre_processor.startswith("来源"):
                pre_processor = re.compile("来源：(\w+)").findall(pre_processor)
                if pre_processor:
                    pre_processor = pre_processor[0]
            else:
                pre_filter = pre_remove.split("来源：")
                filter = []
                for f in pre_filter:
                    if len(f.strip()) != 0 and (not has_digit(f)):
                        filter.append(f)    
                if len(filter) != 0:
                    pre_processor = filter[0]
        elif pre_processor.find("更新时间：") != -1:
            print(456)
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

#个别网站个别处理 lixuepeng
def individual(line, options,web_site):
    if web_site == "辽宁(ln.gov.cn)":
        line = re.compile("信息来源：(\w+)").findall(line)
        return line
    else:
        return []

def normalize(line, options,web_site):
    # if web_site in ["辽宁(ln.gov.cn)","英国(gov.uk)"]:
    #     line = individual(line, options,web_site)
    #     return line
    # else:
    if isinstance(line, str):
        if options == "zh":
            return normalize_zh(line)
        elif options == "en":
            return normalize_en(line)
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
        # "文章来源：2016年第3号    更新时间：2016-07-05 16:52",
        "来源：中国体育报",
        "发文机关：体育总局办公厅",
        "发文机关:   江西省人民政府",
        "来源:  办公室",
        "来源：省教育厅   时间： 2020年09月18日",
        "2017年01月19日    来源：政策法规司"
    ]
    web_site = False
    for line in lines:
        logging.info(normalize(line, "zh",web_site))
