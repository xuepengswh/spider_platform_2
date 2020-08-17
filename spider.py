import requests
import lxml.etree
import selenium.webdriver
from bs4 import BeautifulSoup
import re
import json
from urllib.parse import urljoin
import jsonpath
from selenium.webdriver.chrome.options import Options
import threading
import time
import redis
from pybloom_live import BloomFilter
import cchardet
import configparser

"""
__init__
    任务队列传入实例，默认redis连接方式
__del__
    布隆过滤器保存到数据库
bloom_readfrom_db(self)
bloom_writeto_db(self)
    布隆过滤器读写操作，设计容量100万
    self.tempFile_del = open("tempFile", "wb")
    self.bloomFile = open("tempFile", "rb")
    为了和__del__兼容，上述两个在__init__中实现
get_PageUrlList
    构造新闻的页数链接，page_1,page_2...
get_content_url_list(url)
    获取该网页的所有的新闻链接
download(url）
    下载

jingTai
    存量爬虫
        调用get_PageUrlList获取每个新闻列表
        调用get_content_url_list(url)获取每页的新闻正文链接
        存入布隆过滤器
        存入redis
    增量爬虫
        取一个url判断是否在布隆过滤器，在的话，结束循环
        否则，存入布隆过滤器
        存入redis
update_attr
    根据taskCode获取状态数据
    初始化taskCode，url队列名，redis连接数据库，模板xpath，翻页
start
    任务队列pop任务数据，获取taskCode，并更新self.taskCode 
    更新属性
"""


class Main():
    def __init__(self):
        self.taskCode = ""

        #读取配置文件
        configPath = "config.ini"
        WebConfig = configparser.ConfigParser()
        WebConfig.read(configPath, encoding='utf-8-sig')
        self.redisHost = WebConfig.get("redis", "host")
        self.redisPort = WebConfig.get("redis", "port")
        self.redisPassword = WebConfig.get("redis", "password")
        self.redisDb = WebConfig.get("redis", "database")
        self.redis_platform_address = WebConfig.get("redis","redis_platform_address")

        self.url_key_name = self.redis_platform_address+":url:" + self.taskCode
        self.redis = redis.Redis(host=self.redisHost, port=self.redisPort, decode_responses=True, password=self.redisPassword, db=self.redisDb)

        self.bloom = None

        self.webType = ""
        self.executionTimes =""

        # 页面翻页设置
        self.start_url = ""
        self.second_page_value = ""
        self.end_page_value = ""
        self.url_type = ""
        self.lineListXpath = ""
        # 获取页面元素
        self.titleXpath = ""
        self.contentXpath = ""

        self.proxy = None
        self.header = None
        self.timeout = 10

        self.tempFile_del = open("tempFile", "wb")
        self.bloomFile = open("tempFile", "rb")

    def bloom_readfrom_db(self):
        r = redis.Redis(host=self.redisHost, port=self.redisPort,  password=self.redisPassword, db=self.redisDb)
        bloomDbKeyName = self.redis_platform_address+":bloom:"+self.taskCode
        tempFile = open("tempFile", "wb")
        bloomData = r.get(bloomDbKeyName)
        if bloomData: #如果有布隆过滤器,读取
            tempFile.write(bloomData)
            tempFile.close()
            bloomFile = open("tempFile", "rb")
            self.bloom = BloomFilter.fromfile(bloomFile)
        else:
            self.bloom = BloomFilter(capacity=1000000, error_rate=0.00001)

    def bloom_writeto_db(self):
        r = redis.Redis(host=self.redisHost, port=self.redisPort, password=self.redisPassword, db=self.redisDb)
        bloomDbKeyName = self.redis_platform_address + ":bloom:" + self.taskCode

        self.bloom.tofile(self.tempFile_del)
        self.tempFile_del.close()

        bloomData = self.bloomFile.read()
        r.set(bloomDbKeyName, bloomData)
        self.bloomFile.close()

    def get_PageUrlList(self):
        """构造翻页链接"""
        urlList = [self.url_type % i for i in range(self.second_page_value, self.end_page_value)]
        urlList.append(self.start_url)
        urlList.append(self.start_url)
        return urlList

    def download(self, url):
        _headers = {
            'User-Agent': ('Mozilla/5.0 (compatible; MSIE 9.0; '
                           'Windows NT 6.1; Win64; x64; Trident/5.0)'),
        }
        try:
            if self.proxy:
                response = requests.get(url, proxies=self.proxy, timeout=self.timeout, headers=_headers)
            else:
                response = requests.get(url, timeout=self.timeout, headers=_headers)

            statusCode = response.status_code
            codeStyle = cchardet.detect(response.content)["encoding"]
            webData = response.content.decode(codeStyle, errors="ignore")
            return (webData, statusCode)
        except Exception as e:
            print(e)
            return (0, 0)

    def update_attr(self):
        keyName = self.redis_platform_address+":status:" + self.taskCode  # 获取任务状态键值
        status_data = self.redis.get(keyName)  # 获取所有状态数据
        taskData = json.loads(status_data)

        self.executionTimes = taskData["executionTimes"]
        self.taskCode = taskData["taskCode"]
        self.url_key_name = "mt:spider:platform:url:" + self.taskCode

        self.bloom_readfrom_db()

        # 下载 设置
        if "proxy" in taskData:
            self.proxy = taskData["proxy"]
        if "header" in taskData:
            self.header = taskData["header"]
        if "timeout" in taskData:
            self.timeout = taskData["timeout"]
        if "selenium" in taskData:
            self.selenium = taskData["selenium"]

        temp_data = json.loads(taskData["templateInfo"])    #模板数据
        self.webType = temp_data["webType"]
        # 页面翻页设置
        self.start_url = temp_data["start_url"]
        self.second_page_value = int(temp_data["second_page_value"])
        self.end_page_value = int(temp_data["end_page_value"])
        self.url_type = temp_data["url_type"]
        self.lineListXpath = temp_data["lineListXpath"]

    def get_content_url_list(self, url):
        endUrlList = []
        response = self.download(url)
        if response[1] == 200:
            ps = response[0]
            mytree = lxml.etree.HTML(ps)
            lienlist = mytree.xpath(self.lineListXpath)
            for ii in lienlist:
                endUrl = urljoin(url, ii)
                endUrlList.append(endUrl)
        return endUrlList

    def jingTai(self):
        if self.executionTimes == 1:  # 存量爬虫
            pageList = self.get_PageUrlList()  # 页数链接
            for url in pageList:
                urlList = self.get_content_url_list(url)
                for contentUrl in urlList:
                    self.bloom.add(contentUrl)
                    self.redis.lpush(self.url_key_name, contentUrl)
        else:  # 增量爬虫
            switch = False
            startUrl_urlList = self.get_content_url_list(self.start_url)
            for startUrl_url in startUrl_urlList:   #判断第一页
                print(startUrl_url)
                if startUrl_url in self.bloom:
                    print(startUrl_url,"去重成功")
                    switch = True
                else:
                    self.bloom.add(startUrl_url)
            if not switch:  #判断第二页及以后页数
                for pageIndex in range(self.second_page_value,self.second_page_value):
                    swtich2 = False
                    theUrl = self.url_type % pageIndex  #从第二页开始构造链接
                    second_content_urlList = self.get_content_url_list(theUrl) #每一页的文本链接列表
                    for second_content_url in second_content_urlList:
                        if second_content_url in self.bloom:
                            swtich2 = True
                        else:
                            self.bloom.add(second_content_url)
                            self.redis.lpush(self.url_key_name, second_content_url)
                    if swtich2:
                        break

    def dongTai(self):
        lineListXpath = ""
        url_templace = ""
        for i in range(100000):
            url = url_templace % i
            response = self.download(url)
            if response[1] == 200:
                ps = response[0]
                myjson = json.loads(ps)
                lineList = jsonpath.jsonpath(myjson, lineListXpath)
                if len(lineList) < 1:
                    break
                for lineurl in lineListXpath:
                    contentUrl = urljoin(url, lineurl)

    def start(self):
        while True:
            task_key_name = self.redis_platform_address+":task"
            # tastData = self.redis.lpop(task_key_name)     ###############################################################################################################################################
            tastData = self.redis.lrange(task_key_name,0,0)[0]
            if not tastData:    #如果没有任务，暂停10秒在去一次
                time.sleep(10)
            else:
                print(self.taskCode)
                self.taskCode = json.loads(tastData)["taskCode"]    # 更新self.taskCode
                self.update_attr()  # 更新属性
                if self.webType == 0:
                    self.jingTai()
                else:
                    self.dongTai()
                time.sleep(10)

    def __del__(self):
        self.bloom_writeto_db()  # 布隆过滤器保存到数据库

if __name__ == "__main__":
    mytest = Main()
    mytest.start()


