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

if __name__ == '__main__':
    times = [
        "2020-08-08",
        "2020-08-08 08:08",
        "2020-08-08 08:08:08",
        "2020/08/08",
        "2020/08/08 08:08",
        "2020/08/08 08:08:08",
        "2020 08/08",
        "2020 08/08 08:08",
        "2020 08/08 08:08:08",
        "2020年08月08日",
        "2020年08月08日 08时08分",
        "2020年08月08日 08时08分08秒",
    ]
    for s in times:
        print(get_str(get_date(s)))
