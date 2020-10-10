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

removes = ["成文时间：", "(", ")", "[", "]", "{", "}","成文日期：","成文日期:","成文日期 ：","生成日期:","发布日期 ："]

# 删除removes中包含关键字
def remove_normalize(line):
    result = line

    for r in removes:
        if result.find(r) != -1:
            result = result.replace(r,"")
            
    return False, result.strip()

# 正则提取时间
def extract_time(line):
    result = line
    if line.find("来源：") != -1:
        matcher = re.search(r"[0-9/: \-年月日]+", result, re.I)
        if matcher:
            result = matcher.group(0)
            trim_result = result.strip()
            next_flag = (trim_result.find("日") != -1) and (line.find("日报") != -1)
            if len(trim_result) == 0 or next_flag:
                matcher = re.search(r"[0-9/:\-]+", line)
                if matcher:
                    result = matcher.group(0)
                else:
                    result = line
    return result.strip()

def normalize(line, options):
    line = line.replace("++__))((","")
    if len(line)==13:#时间戳处理
        try:  
            str = int(line)
            timeArray = str/1000
            timeArray = time.localtime(timeArray)
            end_time = time.strftime("%Y-%m-%d", timeArray)
            return [end_time]
        except :
            pass
    extract = extract_time(line.strip())
    label, remove = remove_normalize(extract)
    date = get_date(remove)
    if date:
        return [get_str(date)]
    return []

if __name__ == '__main__':
    lines = [
        # "成文日期：2017-8-24",
        # "成文日期：2019年10月23日",
        # "成文日期 ： 2020-05-14 10:33:11",
        # "生成日期:           2013年12月13日",
        # "成文日期：2020-07-22",
        # "成文日期:   2006-04-14",
        # "成文日期: ++__))(( 2004-05-20",
        # "生成日期:           2020年09月21日",
        "1137052800000",
        "发布日期 ： 2020-08-27 16:35:59"
    ]
    for line in lines:
        logging.info(normalize(line, ""))

