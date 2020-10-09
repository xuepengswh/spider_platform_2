import logging

logging.basicConfig(level=logging.INFO)

removes = ["主    题    词："]
splits = [";;", ";", " "]

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
            r = result.split(s)
            if s == ";;":
                rr = []
                for ri in r:
                    if ri.find(" ") != -1:
                        rr.extend(ri.split(" "))
                    else:
                        rr.append(ri)
                return True, rr
            return True, r
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
        "公路 运输 枢纽 规划 通知",
        "港口规划;管理规定",
        "农村公路;;工作;;意见;;通知",
        "印发 规划;;纲要 通知",
        "主    题    词："
    ]
    for line in lines:
        logging.info(normalize(line, ""))

