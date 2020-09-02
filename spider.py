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
import pymongo
from bson.objectid import ObjectId
import logging
logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG,
                    filename="spider.log")


"""
__init__
    任务队列传入实例，默认redis连接方式
__del__
    布隆过滤器保存到数据库
bloom_readfrom_db(self)
bloom_writeto_db(self)
    布隆过滤器读写操作，设计容量100万
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

        mongoHost = WebConfig.get("mongodb", "host")
        mongoPort = WebConfig.get("mongodb", "port")
        mongoUser = WebConfig.get("mongodb", "user")
        mongoPassword = WebConfig.get("mongodb", "password")
        mongourl = "mongodb://" + mongoUser + ":" + mongoPassword + "@" + mongoHost + ":" + mongoPort
        conn = pymongo.MongoClient(mongourl)
        mongoDatabase = WebConfig.get("mongodb", "database")  # mongo数据库名
        self.myMongo = conn[mongoDatabase]  # 数据库名


        self.bloom = None

        self.webType = ""
        self.executionTimes =""

        # 页面翻页设置
        self.start_url = ""
        self.second_page_value = ""
        self.end_page_value = ""
        self.url_type = ""
        self.lineListXpath = ""
        self.page_xpath = ""    #page页如果有需要提取的数据
        # 获取页面元素
        self.titleXpath = ""
        self.contentXpath = ""

        self.proxy = None
        self.proxy_url = None
        self.header = {
            'User-Agent': ('Mozilla/5.0 (compatible; MSIE 9.0; '
                           'Windows NT 6.1; Win64; x64; Trident/5.0)'),
        }  # header
        self.timeout = 10

    def bloom_readfrom_db(self):
        tempFile = open("tempFile", "wb")

        bloom_dict = self.myMongo["bloom"].find_one({"task_code": self.taskCode})

        if bloom_dict: #如果有布隆过滤器,读取
            bloomData = bloom_dict["bloom_data"]
            tempFile.write(bloomData)
            tempFile.close()
            bloomFile = open("tempFile", "rb")
            self.bloom = BloomFilter.fromfile(bloomFile)
        else:
            self.bloom = BloomFilter(capacity=1000000, error_rate=0.00001)

    def get_proxy(self):
        ps = requests.get(self.proxy_url).text
        return ps

    def bloom_writeto_db(self):
        bloomDbKeyName = self.redis_platform_address + ":bloom:" + self.taskCode

        tempFile_del = open("tempFile", "wb")

        self.bloom.tofile(tempFile_del)         #将布隆过滤器数据写入文件
        tempFile_del.close()

        bloomFile = open("tempFile", "rb")      #打开保存数据的文件
        bloomData = bloomFile.read()

        insert_data = {"task_code": self.taskCode, "bloom_data": bloomData}

        bloom_dict = self.myMongo["bloom"].find_one({"task_code": self.taskCode})

        if bloom_dict:  #更新布隆过滤器
            self.myMongo["bloom"].update_one({"task_code": self.taskCode},{"$set": {"bloom_data":bloomData}})
        else:
            self.myMongo["bloom"].insert_one(insert_data)

        bloomFile.close()
        logging.info(bloomDbKeyName)
        logging.info("布隆过滤器成功保存到数据库")

    def get_PageUrlList(self):
        """构造翻页链接"""
        urlList = [self.url_type % i for i in range(int(self.second_page_value), int(self.end_page_value))]
        urlList.append(self.start_url)
        return urlList

    def download(self, url):

        try:
            if self.proxy == "1":
                proxy = self.get_proxy().strip()
                proxies={'https':proxy}  # 获取代理
                response = requests.get(url, proxies=proxies, timeout=self.timeout, headers=self.header)
                logging.info(url)
                logging.info("以使用代理")
            else:
                response = requests.get(url, timeout=self.timeout, headers=self.header)

            statusCode = response.status_code
            codeStyle = cchardet.detect(response.content)["encoding"]
            webData = response.content.decode(codeStyle, errors="ignore")
            return (webData, statusCode)
        except Exception as e:
            print(e)
            return (0,0)

    def update_attr(self):
        keyName = self.redis_platform_address+":status:" + self.taskCode  # 获取任务状态键值
        status_data = self.redis.get(keyName)  # 获取所有状态数据
        print("-------------------------", self.taskCode)

        taskData = json.loads(status_data)

        self.executionTimes = taskData["executionTimes"]
        self.taskCode = taskData["taskCode"]
        self.url_key_name = self.redis_platform_address+":url:" + self.taskCode



        # 下载 设置
        if "proxy" in taskData:
            self.proxy = taskData["proxy"]
        if "proxyProductValue" in taskData:
            self.proxy_url = taskData["proxyProductValue"]
        if "header" in taskData:
            self.header = taskData["header"]
        if "timeout" in taskData:
            self.timeout = taskData["timeout"]
        if "selenium" in taskData:
            self.selenium = taskData["selenium"]


        temp_data = json.loads(taskData["templateInfo"])    #模板数据
        self.webType = temp_data["web_type"]
        # 页面翻页设置
        self.start_url = temp_data["start_url"]
        self.second_page_value = int(temp_data["second_page_value"])
        self.end_page_value = int(temp_data["end_page_value"])
        self.url_type = temp_data["url_type"]
        self.lineListXpath = temp_data["line_list_xpath"]
        if "page_xpath" in temp_data:
            self.page_xpath = temp_data["page_xpath"]
        else:
            self.page_xpath = ""

    def get_content_url_list(self, url):
        if not self.page_xpath:
            endUrlList = []
            response = self.download(url)
            if response[1] == 200:
                ps = response[0]
                mytree = lxml.etree.HTML(ps)
                linelist = mytree.xpath(self.lineListXpath)
                for ii in linelist:
                    endUrl = urljoin(url, ii)
                    endUrlList.append(endUrl)
            return endUrlList
        else:
            end_data_list = []
            response = self.download(url)
            if response[1] == 200:
                ps = response[0]
                mytree = lxml.etree.HTML(ps)
                linelist = mytree.xpath(self.lineListXpath)
                for line in linelist:
                    one_data_dict = {}
                    for key,keyxpath in self.page_xpath.items():
                        if key == "url_xpath":
                            content_url = line.xpath(keyxpath)
                            if content_url:
                                endUrl = urljoin(url, content_url[0])
                                one_data_dict["url"] = endUrl[::]
                                continue
                            else:   #没有获取到url
                                return

                        keystr = line.xpath(keyxpath)
                        keystr = "".join(keystr)
                        one_data_dict[key] = keystr
                    one_data_dict = json.dumps(one_data_dict)  #将字典转化为字符串
                    end_data_list.append(one_data_dict)
            return end_data_list

    def jingTai(self):
        if self.executionTimes == 1:  # 存量爬虫
            pageList = self.get_PageUrlList()  # 页数链接

            for url in pageList:
                urlList = self.get_content_url_list(url)

                for content_data in urlList:
                    self.bloom.add(content_data)
                    print(self.url_key_name)
                    self.redis.lpush(self.url_key_name, content_data)

        else:  # 增量爬虫
            switch = False
            startUrl_urlList = self.get_content_url_list(self.start_url)
            print(startUrl_urlList)

            for startUrl_url in startUrl_urlList:   #判断第一页
                if startUrl_url in self.bloom:
                    switch = True   # 如果第一页出现以前爬过的url，switch为true，后续的就不在爬了
                else:
                    self.bloom.add(startUrl_url)
                    self.redis.lpush(self.url_key_name, startUrl_url)
                    print(startUrl_url)
                    print(self.url_key_name)

            if not switch:  #判断第二页及以后页数
                for pageIndex in range(int(self.second_page_value),int(self.end_page_value)):
                    swtich2 = False
                    theUrl = self.url_type % pageIndex  #从第二页开始构造链接
                    second_content_urlList = self.get_content_url_list(theUrl) #每一页的文本链接列表
                    for second_content_url in second_content_urlList:
                        if second_content_url in self.bloom:
                            swtich2 = True
                        else:
                            self.bloom.add(second_content_url)
                            self.redis.lpush(self.url_key_name, second_content_url)
                            print(second_content_url)
                    if swtich2:
                        break

        self.bloom_writeto_db()  # 布隆过滤器保存到数据库


    def change_outqueue_num(self):

        keyName = self.redis_platform_address + ":status:" + self.taskCode  # 获取任务状态键值
        status_data = self.redis.get(keyName)  # 获取所有状态数据
        print("-------------------------", self.taskCode)
        taskData = json.loads(status_data)
        taskData["outQueue"] = 1        #更新json数据
        keyname_data = json.dumps(taskData)  # 转化为字符串
        self.redis.set(keyName, keyname_data)  # 更新redis

    def dongTai(self):
        line_list_xpath = ""
        url_templace = ""
        for i in range(100000):
            url = url_templace % i
            response = self.download(url)
            if response[1] == 200:
                ps = response[0]
                myjson = json.loads(ps)
                lineList = jsonpath.jsonpath(myjson, line_list_xpath)
                if len(lineList) < 1:
                    break
                for lineurl in line_list_xpath:
                    contentUrl = urljoin(url, lineurl)

    def start(self):
        while True:
            task_key_name = self.redis_platform_address+":task"
            tastData = self.redis.lpop(task_key_name)
            # tastData = self.redis.lrange(task_key_name,0,0)[0]
            if not tastData:    #如果没有任务，暂停10秒在去一次
                time.sleep(10)
            else:
                print(self.taskCode)
                self.taskCode = json.loads(tastData)["taskCode"]    # 更新self.taskCode
                self.change_outqueue_num()      #更改outQueue值为1
                print(tastData)
                self.update_attr()  # 更新属性
                self.bloom_readfrom_db()  # 更新布隆过滤器
                if self.webType == 0:
                    self.jingTai()
                else:
                    self.dongTai()
                time.sleep(10)


if __name__ == "__main__":
    mytest = Main()
    mytest.start()
