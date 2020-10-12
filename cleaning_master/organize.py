import re
import logging

removes = ["发文机关:","发文机构 ：","发文单位：","发文机关：","发布机构:","【发布单位】"]
split_str = ["、"," "]

# def split_normalize(line):
#     result = []
#     for ii in split_str:
#         if line.find(ii)!=-1:
#             split_list = line.split(ii)
#             for one_ii in split_list:
#                 if not one_ii.has


def remove_normalize(line): # 删除关键字
    result = line
    for r in removes:
        if result.find(r) != -1:
            result = result.strip(r)
    return result.strip()

def go_split():
    pass

def re_get_orgnize(line): #正则表达式取出来源后面的文字信息
    result = line
    if line.find("来源") != -1:
        matcher = re.search(r"来源[:：]([\s]*[\u4e00-\u9fa5]*)", result, re.I)
        if matcher:
            result = matcher.group(1)
        else:
            result = line
    return result.strip()

def normalize(line, options):
    line = line.replace("++__))((","")
    re_org = re_get_orgnize(line.strip())   #正则表达式取出来源后面的文字信息
    remove = remove_normalize(re_org)   # 删除关键字
    trim = remove.strip()
    return [trim]

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
        "文章来源：2016年第3号    更新时间：2016-07-05 16:52"
    ]
    for line in lines:
        print(normalize(line, ""))
