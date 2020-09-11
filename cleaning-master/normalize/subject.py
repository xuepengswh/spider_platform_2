import logging

logging.basicConfig(level=logging.INFO)

removes = ["主 题 词", "主题分类："]
splits = ["\\\\", "\\", "/", ";", "  ", " "]

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

def normalize(line, options):
    remove = remove_normalize(line.strip().replace("\xa0", " "))
    trim = remove.strip()
    if len(trim) != 0:
        label, split = split_normalize(trim)
        if label:
            return split
    return [trim]

if __name__ == '__main__':
    lines = [
        "主 题 词 环保 信息 规范 公告",
        "财政、金融、审计\\其他",
        "部颁规章/环境监测、保护与治理",
        "工业、交通\\信息产业(含电信)",
        "主 题 词",
        "主题分类：",
        "通知  各类社会公众  资本项目管理"
    ]
    for line in lines:
        logging.info(normalize(line, ""))