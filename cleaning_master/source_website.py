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

removes = ["发布机构：", "文章来源：", "来源：", "时间：", "《", "》"]
splits = ["/", " ", "、", "||", "－"]

# 删除removes中包含关键字
def remove_normalize(line):
    result = line.replace(" ","")
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
    return re.search(r'\d+', processor)

# 中文组织信息归一化
def normalize_zh(line):
    pre_remove = remove_normalize(line.strip().replace("\xa0", " "))
    pre_processor = pre_remove
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

def individual(line, options,web_site): #个别网站个别处理
    if web_site == "国务院新闻办公室(scio.gov.cn)":
        line = re.compile("来源：(\w+)").findall(line)
        return line
    else:
        return []

def normalize(line, options,web_site):
    if web_site in ["国务院新闻办公室(scio.gov.cn)"]:
        line = individual(line, options,web_site)
        return line
    else:
        if isinstance(line, str):
            if options == "zh":
                return normalize_zh(line)
            elif options == "en":
                return normalize_en(line)
        return [line]

if __name__ == '__main__':
    lines = [
        "来源：中国体育报"
    ]
    web_site = False
    for line in lines:
        logging.info(normalize(line, "zh", web_site))
