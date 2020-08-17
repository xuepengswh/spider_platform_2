from flask import Flask, request
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
import logging
import cchardet
import hashlib
import configparser

"""
__init__
	初始化连接redis数据库，以后不变
	初始化xpath参数，网站属性(网站名等)，下载设置，除了默认值，都设置为Node或者""，后面调用函数修改参数
get_proxy(self)
    另开一个线程，没间隔5秒更新一下代理数据
updata_attr(一般为redis的status数据)
	根据传入参数更新属性
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
        self.task_code = None  # 在start 和get_task_status函数中更新
        self.task_status = None
        self.webSite = ""
        self.langCode = ""

        # 页面元素初始化
        self.titleXpath = None
        self.contentXpath = None
        self.industrialClassXpath = None
        self.organizationXpath = None
        self.countryNameXpath = None
        self.policyIndexNumberXpath = None
        self.issuedNumberXpath = None
        self.subjectClassXpath = None
        self.statementTimeXpath = None
        self.htmlContentXpath = None
        self.abstractContentXpath = None
        self.regionProvinceXpath = None
        self.otherClassXpath = None

        # 下载 设置初始化
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

    def updata_attr(self):
        # self.task_code  在start 和get_task_status函数中更新
        tempData = json.loads(self.task_status["templateInfo"])

        # 需要存储数据
        self.webSite = self.task_status["webSiteName"]
        self.langCode = self.task_status["langInfoKey"]

        # 页面元素设置
        xpathData = tempData["xpaths"]
        self.titleXpath = xpathData["titleXpath"]
        self.contentXpath = xpathData["contentXpath"]
        self.htmlContentXpath = xpathData["htmlContentXpath"]
        if "industrialClassXpath" in xpathData:
            self.industrialClassXpath = xpathData["industrialClassXpath"]
        if "organizationXpath" in xpathData:
            self.organizationXpath = xpathData["organizationXpath"]
        if "countryNameXpath" in xpathData:
            self.countryNameXpath = xpathData["countryNameXpath"]
        if "policyIndexNumberXpath" in xpathData:
            self.policyIndexNumberXpath = xpathData["policyIndexNumberXpath"]
        if "issuedNumberXpath" in xpathData:
            self.issuedNumberXpath = xpathData["issuedNumberXpath"]
        if "subjectClassXpath" in xpathData:
            self.subjectClassXpath = xpathData["subjectClassXpath"]
        if "statementTimeXpath" in xpathData:
            self.statementTimeXpath = xpathData["statementTimeXpath"]
        if "abstractContentXpath" in xpathData:
            self.abstractContentXpath = xpathData["abstractContentXpath"]
        if "regionProvinceXpath" in xpathData:
            self.regionProvinceXpath = xpathData["regionProvinceXpath"]
        if "otherClassXpath" in xpathData:
            self.otherClassXpath = xpathData["otherClassXpath"]

        # 更新下载设置状态
        threadNum = int(self.task_status["threadNum"])  # 线程数量设置，默认是1个线程
        self.thread_num = threadNum
        # 设置代理
        self.proxy = self.task_status["useProxy"]
        self.proxy_url = self.task_status["proxyProductValue"]
        self.proxy_dict = ""

        self.timeInterval = self.task_status["timeInterval"]
        self.storeQueue = self.task_status["storeQueue"]

        # 更新模板设置状态
        if "header" in tempData:
            self.header = tempData["header"]
        if "timeout" in tempData:
            self.timeout = tempData["timeout"]
        if "selenium" in tempData:
            self.selenium = tempData["selenium"]

    def get_proxy(self):
        while True:
            ps = requests.get(self.proxy_url).text
            self.proxy_dict = json.loads(ps)
            time.sleep(5)

    def insert_data(self, data):
        data = json.dumps(data)
        print(data)
        if self.storeQueue == "1":  # 存储到redis和mongodb
            consStore_url_key_name = self.redis_platform_address+":constant:" + self.task_code  # 永久的redis存储
            self.redis.lpush(consStore_url_key_name, data)
            pass
        tempStore_url_key_name = self.redis_platform_address+":temporary"  # 暂时的存储
        self.redis.lpush(tempStore_url_key_name, data)

    def get_url_key_name(self):
        url_key_find = self.redis_platform_address+":url:*"
        url_key_name_list = self.redis.keys(url_key_find)
        return url_key_name_list

    def get_task_status(self, url_key_name):
        self.task_code = url_key_name.split(":")[-1]  # 更新self.task_code
        keyName = self.redis_platform_address+":status:" + self.task_code
        status_data = self.redis.get(keyName)  # 获得所有状态
        return status_data

    def get_content(self, url):
        """获取文本内容"""
        print(url)
        response = self.download(url)
        if response[1] == 200:
            ps = response[0]
            mytree = lxml.etree.HTML(ps)

            title = mytree.xpath(self.titleXpath)  # title
            if title:
                title = title[0]
                title = title.replace("\r", " ").replace("\n", " ")
            else:
                title = ""

            hashTitle = hashlib.md5(title.encode())
            hashTitle = hashTitle.hexdigest().upper()

            plist = mytree.xpath(self.contentXpath)  # content
            content = " ".join(plist)
            content = content.replace("\r", " ").replace("\n", " ")

            html_content = mytree.xpath(self.htmlContentXpath)  # html_content
            if html_content:
                html_content = lxml.etree.tostring(html_content[0], pretty_print=True, method='html', encoding='utf-8')
                codeStyle = cchardet.detect(html_content)["encoding"]
                html_content = html_content.decode(codeStyle, errors="ignore")
            else:
                html_content = ""

            if self.industrialClassXpath:
                industrialClass = mytree.xpath(self.industrialClassXpath)
                if industrialClass:
                    industrialClass = industrialClass[0]
                else:
                    industrialClass = ""
            else:
                industrialClass = ""
            if self.organizationXpath:
                organization = mytree.xpath(self.organizationXpath)
                if organization:
                    organization = organization[0]
                else:
                    organization = ""
            else:
                organization = ""
            if self.countryNameXpath:
                countryName = mytree.xpath(self.countryNameXpath)
                if countryName:
                    countryName = countryName[0]
                else:
                    countryName = ""
            else:
                countryName = ""
            if self.policyIndexNumberXpath:
                policyIndexNumber = mytree.xpath(self.policyIndexNumberXpath)
                if policyIndexNumber:
                    policyIndexNumber = policyIndexNumber[0]
                else:
                    policyIndexNumber = ""
            else:
                policyIndexNumber = ""
            if self.issuedNumberXpath:
                issuedNumber = mytree.xpath(self.issuedNumberXpath)
                if issuedNumber:
                    issuedNumber = issuedNumber[0]
                else:
                    issuedNumber = ""
            else:
                issuedNumber = ""
            if self.subjectClassXpath:
                subjectClass = mytree.xpath(self.subjectClassXpath)
                if subjectClass:
                    subjectClass = subjectClass[0]
                else:
                    subjectClass = ""
            else:
                subjectClass = ""
            if self.statementTimeXpath:
                statementTime = mytree.xpath(self.statementTimeXpath)
                if statementTime:
                    statementTime = statementTime[0]
                else:
                    statementTime = ""
            else:
                statementTime = ""
            if self.abstractContentXpath:
                abstractContent = mytree.xpath(self.abstractContentXpath)
                if abstractContent:
                    abstractContent = abstractContent[0]
                else:
                    abstractContent = ""
            else:
                abstractContent = ""
            if self.regionProvinceXpath:
                regionProvince = mytree.xpath(self.regionProvinceXpath)
                if regionProvince:
                    regionProvince = regionProvince[0]
                else:
                    regionProvince = ""
            else:
                regionProvince = ""
            if self.otherClassXpath:
                otherClass = mytree.xpath(self.otherClassXpath)
                if otherClass:
                    otherClass = otherClass[0]
                else:
                    otherClass = ""
            else:
                otherClass = ""

            endData = {"url": url,
                       "webSite": self.webSite,
                       "title": title,
                       "titleMD5": hashTitle,
                       "langCode": self.langCode,
                       "industrialClass": industrialClass,
                       "organization": organization,
                       "countryName": countryName,
                       "policyIndexNumber": policyIndexNumber,
                       "issuedNumber": issuedNumber,
                       "subjectClass": subjectClass,
                       "statementTime": statementTime,
                       "htmlContent": html_content,
                       "content": content,
                       "abstractContent": abstractContent,
                       "regionProvince": regionProvince,
                       "otherClass": otherClass,
                       "taskCode": self.task_code}
            self.insert_data(endData)  # 存储数据
            return


        else:  # 状态码不是200
            return

    def download(self, url):
        try:
            if self.proxy == "1":  # 使用代理
                proxy_thread = threading.Thread(target=self.get_proxy)  # 开启代理线程
                proxy_thread.start()
                time.sleep(1)  # 获取代理
                response = requests.get(url, proxies=self.proxy_dict, timeout=self.timeout, headers=self.header)
            else:  # 不适用代理
                response = requests.get(url, timeout=self.timeout, headers=self.header)
            code_style = cchardet.detect(response.content)["encoding"]
            webData = response.content.decode(code_style, errors="ignore")
            statusCode = response.status_code
            return (webData, statusCode)
        except Exception as e:
            print(e)
            return (0, 0)

    def thread_start(self, urlList):
        threadList = []
        for url in urlList:
            mythread = threading.Thread(target=self.get_content, args=(url,))
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

                self.updata_attr()  # 更新属性值，获取下载方式及页面xpath
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

