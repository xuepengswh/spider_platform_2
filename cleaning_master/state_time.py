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
    u"%Y年%m月%d日  %H时%M分%S秒",
    u"%Y.%m.%d"
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

removes = ["更新时间：", "发布时间：", "(", ")", "[", "]", "{", "}","发布日期："]

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
    extract = extract_time(line.strip()) #去除两边空格
    label, remove = remove_normalize(extract)   #去除关键字
    date = get_date(remove)
    if date:
        return [get_str(date)]
    return []

if __name__ == '__main__':
    lines = [
        "2019-12-16",
        "2020年03月31日",
        "[2012-06-01]",
        "2011-03-15 13:28:00",
        "2020-06-12 16:13",
        "更新时间：2015-04-29 11:21",
        "发布时间：2020-01-30",
        "2018 08/13 16:26",
        "2019年10月15日 07时51分",
        "2018 08/13 16:26 来源：新疆政府网",
        "来源：中共云南省委政策研究室 2020-05-20 11:45:00",
        "1998-01-01  00:00",
        "2017年02月09日    来源：政策法规司",
        "2018-06-21  13:53                        来源：江苏省水利厅",
        "2002-10-01  00:00",
        "2016 05/03 00:00 来源： 乌鲁木齐市政府 【字体： 】 访问量： 次",
        "来源：云南日报                    2016-04-18 07:16:00",
        "中文",
        "2020年",
        "发布日期：2018-11-08",
        "2020.05.20 ",
        "2019-11-27 07:12:32"
    ]
    for line in lines:
        logging.info(normalize(line, ""))
