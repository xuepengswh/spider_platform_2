import re
import logging

removes = ["索  引  号:","索 引 号：","索  引  号:"]

# 删除removes中包含关键字
def remove_normalize(line):
    result = line
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
        "索 引 号：191984",
        "索  引  号:123"
    ]
    for line in lines:
        print(normalize(line, ""))
