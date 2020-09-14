from flask import Flask, request
import requests
import lxml.etree
from selenium import webdriver
from bs4 import BeautifulSoup
import re
import json
from urllib.parse import urljoin
import jsonpath
from selenium.webdriver.chrome.options import Options
import threading
import time
import redis
import logging
import cchardet
import hashlib
import configparser
import logging
import pymongo
import uuid
from bson.objectid import ObjectId
from cleaning_master import time as clean_time
from cleaning_master import subject as clean_subject
from cleaning_master import organize as clean_organize
from cleaning_master import category as clean_category
from cleaning_master import industrial as clean_industrial
logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG,
                    filename="spider.log")


"""
__init__
	初始化连接redis数据库，以后不变
	初始化xpath参数，网站属性(网站名等)，下载设置，除了默认值，都设置为Node或者""，后面调用函数修改参数
get_proxy(self)
    另开一个线程，没间隔5秒更新一下代理数据
updata_attr(一般为redis的status数据)
	根据传入参数更新属性
	更改状态为进行中
insert_data(存储数据data)
	将传入参数存储到redis临时数据库，个别还需要存储到永久数据库，还需要taskCode
get_url_key_name
	获取redis的url队列的所有键的名字，并返回列表
get_task_status(url_key_name既是url队列的键名)
	根据键名获取taskCode并更新
	根据构造url状态队列的键名，并获取状态数据并返回
get_content(url)
	掉用download函数下载url，
	需要字段，构造数据
	调用insert_data函数存储数据
download(url)
    如果需要代理，先开启代理线程，等待一秒，更新代理数据
	下载数据
thread_start(urlList)
	根据urllist创建下载队列并并发开始下载
get_key_name(url_key_name_list)
    根据url_key_name_list依次取键名，调用get_task_status获取状态数据,
    1，没有找到状态数据，说明任务被删除，删除url队列即可
    2，找到状态，调用updata_attr更新属性
    3，任务停止-删除url
    4，任务未开始或进行中，返回key_name
    5，其他的已完成，已失败，停止等状态，直接不管
start
	获取redis所有队列名
	如果队列名为空，暂停10秒重新开始
    调用get_key_name函数，返回值为空，从头开始循环

	根据线程数，从url队列获取url，构造urlList
		如果url队列为空，从头开始执行
	开启多线程下载
	暂停一段时间后再次开始此次循环，时间间隔可设定
"""


class Main():
    def __init__(self):
        # 读取配置文件
        configPath = "config.ini"
        WebConfig = configparser.ConfigParser()
        WebConfig.read(configPath, encoding='utf-8-sig')
        self.redisHost = WebConfig.get("redis", "host")
        self.redisPort = WebConfig.get("redis", "port")
        self.redisPassword = WebConfig.get("redis", "password")
        self.redisDb = WebConfig.get("redis", "database")
        self.redis_platform_address = WebConfig.get("redis", "redis_platform_address")
        self.redis = redis.Redis(host=self.redisHost, port=self.redisPort, decode_responses=True, password=self.redisPassword, db=self.redisDb)

        mongoHost = WebConfig.get("mongodb", "host")
        mongoPort = WebConfig.get("mongodb", "port")
        mongoUser = WebConfig.get("mongodb", "user")
        mongoPassword = WebConfig.get("mongodb", "password")
        mongoDatabase = WebConfig.get("mongodb", "database")
        mongourl = "mongodb://" + mongoUser + ":" + mongoPassword + "@" + mongoHost + ":" + mongoPort
        conn = pymongo.MongoClient(mongourl)
        self.myMongo = conn[mongoDatabase]  # 数据库名

        self.task_code = None  # 在start 和get_task_status函数中更新
        self.task_status = None
        self.webSite = ""
        self.langCode = ""

        # 下载 设置初始化
        self.storeQueueKey = None
        self.timeInterval = 0  # 时间间隔
        self.thread_num = 1  # 线程数
        self.proxy = "0"  # 代理
        self.header = {
            'User-Agent': ('Mozilla/5.0 (compatible; MSIE 9.0; '
                           'Windows NT 6.1; Win64; x64; Trident/5.0)'),
        }  # header
        self.storeQueue = "0"  # 只存储到mongodb
        self.timeout = 20  # 下载最长延时
        self.selenium = None  # 是否使用selenium
        self.driver = None
        self.xpath_data = ""
        self.templateCode = ""

    def updata_attr(self):
        # self.task_code  在start 和get_task_status函数中更新
        tempData = json.loads(self.task_status["templateInfo"])

        # 需要存储数据
        self.webSite = self.task_status["webSiteName"]
        self.langCode = self.task_status["langInfoKey"]

        # 页面元素设置
        xpathData = tempData["xpaths"]
        self.xpath_data = xpathData


        # 更新下载设置状态
        threadNum = int(self.task_status["threadNum"])  # 线程数量设置，默认是1个线程
        self.thread_num = threadNum
        # 设置代理
        self.proxy = self.task_status["useProxy"]
        self.proxy_url = self.task_status["proxyProductValue"]

        self.timeInterval = self.task_status["timeInterval"]
        self.storeQueue = self.task_status["storeQueue"]
        self.storeQueueKey = self.task_status["storeQueueKey"]
        self.templateCode = self.task_status["templateCode"]

        # 更新模板设置状态
        if "header" in tempData:
            self.header = tempData["header"]
        if "timeout" in tempData:
            self.timeout = tempData["timeout"]
        if "selenium" in tempData:
            self.selenium = tempData["selenium"]
            opt = webdriver.ChromeOptions()
            opt.set_headless()
            self.driver = webdriver.Chrome(options=opt)



    def change_status_running(self):
        mongo_id = self.task_status["id"]  # 获取id值
        status_num_data = self.myMongo["task_info"].find_one({"_id": ObjectId(mongo_id)})
        if status_num_data:
            status_num = int(status_num_data["status"])
            if status_num == 2:
                pass
            else:
                self.myMongo["task_info"].update_one({"_id": ObjectId(mongo_id)}, {"$set": {"status": "2"}})
                logging.info(ObjectId(mongo_id))
                logging.info("将此数据更改为进行中状态")

    def get_proxy(self):
        ps = requests.get(self.proxy_url).text
        return ps

    def clean_data(self,data):
        if "statement_time_source" in data:
            data["statement_time"] = "".join( clean_time.normalize(data["statement_time_source"],"")  )
        if "written_time_source" in data:
            data["written_time"] = "".join( clean_time.normalize(data["written_time_source"],"")  )
        if "organization_source" in data:
            data["organization"] = "".join( clean_organize.normalize(data["organization_source"],"zh")  )
        if "subject_class_source" in data:
            data["subject_class"] = "".join(  clean_subject.normalize(data["subject_class_source"],"")  )
        if "industrial_class_source" in data:
            data["industrial_class"] = "".join(  clean_industrial.normalize(data["industrial_class_source"],"")  )
        if "category_word_source" in data:
            data["category_word"] = "".join(   clean_category.normalize(data["category_word_source"],"")  )
        return data


    def insert_data(self, data):
        data = self.clean_data(data)
        tempData = json.loads(self.task_status["templateInfo"])
        if "constant_filed" in tempData:
            for key,value in tempData["constant_filed"].items():    # 增加 永久存储字段
                data[key] = value
        data["template_code"] = self.templateCode

        if self.storeQueue == "1":  # 存储到redis和mongodb
            data_id = data["_id"]
            redis_store_data = {"id":data_id,"collectionName":self.task_code}
            redis_store_data = json.dumps(redis_store_data)
            self.redis.lpush(self.storeQueueKey, redis_store_data)
            self.myMongo[self.task_code].insert_one(data)
        else:   #存储到mongo
            self.myMongo[self.task_code].insert_one(data)


    def get_url_key_name(self):
        url_key_find = self.redis_platform_address+":url:*"
        url_key_name_list = self.redis.keys(url_key_find)
        return url_key_name_list

    def get_task_status(self, url_key_name):
        self.task_code = url_key_name.split(":")[-1]  # 更新self.task_code
        keyName = self.redis_platform_address+":status:" + self.task_code
        status_data = self.redis.get(keyName)  # 获得所有状态
        return status_data

    def get_content(self, url, page_data):  # page_data如果有的话，是linelist页的数据
        """获取文本内容"""
        if self.selenium:
            response = self.selenium_download(url)
        else:
            response = self.download(url)
        if response[1] == 200:
            ps = response[0]
            mytree = lxml.etree.HTML(ps)

            endData = {}

            for key, keyxpath in self.xpath_data.items():

                if type(keyxpath) == int or (not keyxpath.startswith(r"//")):
                    continue

                if key == "html_content_xpath":  # htmlContentXpath单独处理
                    total_html_content = mytree.xpath(keyxpath)  # html_content
                    endcontent = ""
                    for one_content in total_html_content:
                        html_content = lxml.etree.tostring(one_content, encoding="utf-8", pretty_print=True,
                                                           method="html")
                        codeStyle = cchardet.detect(html_content)["encoding"]
                        html_content = html_content.decode(codeStyle, errors="ignore")
                        endcontent += html_content

                    endData["html_content"] = endcontent

                    #下面是从html_content中提取content
                    endcontent = re.compile("<script[^>]*?>[\\s\\S]*?<\\/script>").sub("", endcontent)
                    endcontent = re.compile("<!--[\\s\\S]*?-->").sub("", endcontent)
                    endcontent = re.compile("<style[^>]*?>[\\s\\S]*?<\\/style>").sub("", endcontent)
                    endcontent = re.compile("<[^>]+>").sub("", endcontent)
                    endcontent = re.compile("\\s*|\t|\r|\n|　*").sub("", endcontent)
                    endData["content"] = endcontent

                    continue
                keystr = mytree.xpath(keyxpath)



                keystr = " ".join(keystr)
                keystr = keystr.replace("\n", " ").replace("\t", " ").replace("\r", " ")
                keystr = keystr.strip()
                key = key.replace("_xpath","")

                if key == "content":    #content已经从htmlcontent中提取出来了
                    continue

                if key == "title":
                    hashTitle = hashlib.md5(keystr.encode())
                    hashTitle = hashTitle.hexdigest().upper()
                    endData["titleMD5"] = hashTitle
                endData[key] = keystr


            endData["_id"] = str(uuid.uuid4())
            endData["url"] = url
            endData["web_site"] = self.webSite
            endData["language_type"] = self.langCode
            endData["task_code"] = self.task_code

            if page_data:   #判断有没有其他数据
                for key, value in page_data.items():
                    if key == "url_xpath":
                        continue
                    key = key.replace("_xpath","")
                    endData[key] = value

            print("1" * 100)
            print(endData)
            time.sleep(10)

            self.insert_data(endData)  # 存储数据
        else:  # 状态码不是200
            return

    def download(self, url):
        try:
            if self.proxy == "1":  # 使用代理
                proxy = self.get_proxy().strip()
                proxies={'https':proxy}  # 获取代理
                response = requests.get(url, proxies=proxies, timeout=self.timeout, headers=self.header,verify=False)
                logging.info(url)
                logging.info("以使用代理")
            else:  # 不适用代理
                response = requests.get(url, timeout=self.timeout, headers=self.header,verify=False)
            code_style = cchardet.detect(response.content)["encoding"]
            webData = response.content.decode(code_style, errors="ignore")
            statusCode = response.status_code
            return (webData, statusCode)
        except Exception as e:
            print(e)
            return (0, 0)

    def selenium_download(self, url):
        self.driver.get(url)
        ps =self.driver.page_source
        self.driver.quit()
        return (ps,200)

    def thread_start(self, urlList):
        threadList = []
        for url in urlList:
            if not url.startswith("{"):    #redis取出数据为单纯的url
                mythread = threading.Thread(target=self.get_content, args=(url,None))
                threadList.append(mythread)
            else:   #redis取出数据为字典
                url = json.loads(url)#将提取数据转化为json格式
                content_url = url["url"]
                mythread = threading.Thread(target=self.get_content, args=(content_url,url))
                threadList.append(mythread)
        for i in threadList:
            i.start()
        for i in threadList:
            i.join()

    def get_key_name(self, url_key_name_list):
        endName = None
        for url_key_name in url_key_name_list:
            task_status = self.get_task_status(url_key_name)  # 获取该任务的状态，为字符串格式,更新self.task_code
            if task_status is None:  # 如果没有找到状态，删除redis的url
                self.redis.delete(url_key_name)
                continue  # 找下一个键名
            status_json = json.loads(task_status)
            self.task_status = status_json
            status_num = int(self.task_status["status"])  # 获取状态码
            #   任务停止-删除url
            #   任务未开始或进行中，返回key_name
            #   其他的已完成，已失败，暂停等状态，直接不管
            if status_num == 4:  # 任务停止
                self.redis.delete(url_key_name)
            if status_num == 1 or status_num == 2:
                endName = url_key_name
                break  # 取得一个键名直接返回
        return endName

    def start(self):
        while True:  # 不设置间隔，一直循环获取链接
            url_key_name_list = self.get_url_key_name()  # 获取所有存储链接的队列名
            if len(url_key_name_list) == 0:  # 如果没有队列名，暂停10秒在执行一次
                time.sleep(10)

            else:
                url_key_name = self.get_key_name(url_key_name_list)  # 获取键名以及对应的状态
                if not url_key_name:  # 键名为空
                    time.sleep(3)  # 暂停3秒
                    continue  # 结束，开启下一个循环

                self.updata_attr()  # 更新属性值，获取下载方式
                self.change_status_running()    #更改状态为进行中

                tasklist = []  # url队列
                for i in range(self.thread_num):  # 添加线程数个url
                    if self.redis.llen(url_key_name) == 0:  # 如果url队列为空，返回
                        break
                    currentUrl = self.redis.lpop(url_key_name)  # redis队列pop一个url
                    print(currentUrl)
                    tasklist.append(currentUrl)

                if not tasklist:  # 如果线程队列为空，结束，从头开始执行
                    continue
                self.thread_start(tasklist)  # 开启多线程下载

                time.sleep(int(self.timeInterval))  # 时间间隔


if __name__ == "__main__":
    mytest = Main()
    mytest.start()

