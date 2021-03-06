import re
import logging

removes = ["发文字号：","文号:"]

# 删除removes中包含关键字
def remove_normalize(line):
    result = line.replace(" ","")
    for r in removes:
        if result.find(r) != -1:
            result = result.strip(r)
    return result.strip()

def normalize(line, options):
    remove = remove_normalize(line.strip().replace("\xa0", " "))
    trim = remove.strip()
    return [trim]

if __name__ == '__main__':
    lines = [
        "发文字号：体群字〔2019〕17号",
        "文      号: 123"
    ]
    for line in lines:
        print(normalize(line, ""))
