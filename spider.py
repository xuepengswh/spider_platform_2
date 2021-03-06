import requests
import lxml.etree
import re
import json
from urllib.parse import urljoin
from urllib import parse
import jsonpath
import time
import redis
from pybloom_live import BloomFilter
import cchardet
import configparser
import pymongo
import logging

logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG,
                    filename="spider925.log")


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
        self.executionType =""

        # 页面翻页设置
        self.start_url = ""
        self.second_page_value = ""
        self.page_interval = ""
        self.end_page_value = ""
        self.url_type = ""
        self.lineListXpath = ""
        self.json_page_re = ""
        self.page_xpath = ""    #page页如果有需要提取的数据
        # 获取页面元素
        self.titleXpath = ""
        self.contentXpath = ""

        self.proxy = None
        self.proxy_url = None
        self.headers = {
            'User-Agent': ('Mozilla/5.0 (compatible; MSIE 9.0; '
                           'Windows NT 6.1; Win64; x64; Trident/5.0)'),
        }  # header
        self.timeout = 10
        self.timeInterval = 0  # 时间间隔
        self.post_data = ""
        self.page_num_str = ""
    # 从数据库读布隆过滤器数据
    def bloom_readfrom_db(self):
        tempFile = open("tempFile", "wb")

        bloom_dict = self.myMongo["bloom"].find_one({"_id": self.taskCode})

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

    # 将布隆过滤器数据写入数据库保存
    def bloom_writeto_db(self):
        bloomDbKeyName = self.redis_platform_address + ":bloom:" + self.taskCode

        tempFile_del = open("tempFile", "wb")

        self.bloom.tofile(tempFile_del)         #将布隆过滤器数据写入文件
        tempFile_del.close()

        bloomFile = open("tempFile", "rb")      #打开保存数据的文件
        bloomData = bloomFile.read()

        insert_data = {"_id": self.taskCode, "bloom_data": bloomData}

        bloom_dict = self.myMongo["bloom"].find_one({"_id": self.taskCode})

        if bloom_dict:  #更新布隆过滤器
            self.myMongo["bloom"].update_one({"_id": self.taskCode},{"$set": {"bloom_data":bloomData}})
        else:
            self.myMongo["bloom"].insert_one(insert_data)

        bloomFile.close()
        logging.info("布隆过滤器成功保存到数据库"+bloomDbKeyName)

    # 构造链接页的所有链接
    def get_PageUrlList(self):
        """构造翻页链接"""
        urlList = []
        for i in range(int(self.second_page_value), int(self.end_page_value)):
            page_num = str(i)
            page_url = self.url_type.replace("%d", page_num)
            urlList.append(page_url)
        urlList.append(self.start_url)
        return urlList

    #根据url下载数据
    def download(self, url):
        try:
            if self.proxy:
                proxy = self.get_proxy().strip()
                proxies={'https':proxy}  # 获取代理
                response = requests.get(url, proxies=proxies, timeout=self.timeout, headers=self.headers,verify=False)
                logging.info(url)
                logging.info("以使用代理")
            else:
                response = requests.get(url, timeout=self.timeout, headers=self.headers,verify=False)

            statusCode = response.status_code
            codeStyle = cchardet.detect(response.content)["encoding"]
            if not codeStyle:
                codeStyle = "utf-8"
            webData = response.content.decode(codeStyle, errors="ignore")
            return (webData, statusCode)
        except Exception as e:
            print(e)
            return (0,0)

    def change_outqueue_num(self):
        keyName = self.redis_platform_address + ":status:" + self.taskCode  # 获取任务状态键值
        status_data = self.redis.get(keyName)  # 获取所有状态数据
        print("-------------------------", self.taskCode)
        taskData = json.loads(status_data)
        taskData["outQueue"] = 1        #更新json数据
        keyname_data = json.dumps(taskData)  # 转化为字符串
        self.redis.set(keyName, keyname_data)  # 更新redis

    # 更新所有需要的属性
    def update_attr(self):
        keyName = self.redis_platform_address+":status:" + self.taskCode  # 获取任务状态键值
        status_data = self.redis.get(keyName)  # 获取所有状态数据
        print("-------------------------", self.taskCode)

        taskData = json.loads(status_data)

        self.executionType = int(taskData["executionType"])
        self.taskCode = taskData["taskCode"]
        self.timeInterval = taskData["timeInterval"]
        self.url_key_name = self.redis_platform_address+":url:" + self.taskCode



        # 下载 设置
        if "proxy" in taskData:
            self.proxy = taskData["proxy"]
        else:
            self.proxy = ""
        if "proxyProductValue" in taskData:
            self.proxy_url = taskData["proxyProductValue"]
        else:
            self.proxy_url = ""

        if "timeout" in taskData:
            self.timeout = taskData["timeout"]
        else:
            self.timeout = 10


        temp_data = json.loads(taskData["templateInfo"])    #模板数据
        print(temp_data)
        try:
            self.webType = temp_data["web_type"]
        except KeyError:
            self.webType = temp_data["webType"]

        # 页面翻页设置
        self.start_url = temp_data["start_url"]
        self.second_page_value = int(temp_data["second_page_value"])
        if "page_interval" in temp_data:
            self.page_interval = int(temp_data["page_interval"])
        else:
            self.page_interval = 1
        self.end_page_value = int(temp_data["end_page_value"])
        self.url_type = temp_data["url_type"]
        try:
            self.lineListXpath = temp_data["line_list_xpath"]
        except KeyError:
            self.lineListXpath = temp_data["lineListXpath"]

        if "headers" in temp_data:
            self.headers = temp_data["headers"]
        else:
            self.headers = {
            'User-Agent': ('Mozilla/5.0 (compatible; MSIE 9.0; ''Windows NT 6.1; Win64; x64; Trident/5.0)'),
        }  # header

        if "json_page_re" in temp_data:
            self.json_page_re = temp_data["json_page_re"]
        else:
            self.json_page_re = ""
        if "post" in temp_data:
            self.post_data = temp_data["post"]
        else:
            self.post_data = None
        if "page_num_str" in temp_data:
            self.page_num_str = temp_data["page_num_str"]
        else:
            self.page_num_str = ""

        if "page_xpath" in temp_data:
            self.page_xpath = temp_data["page_xpath"]
        else:
            self.page_xpath = ""

    def deal_html_page_data(self,base_url,line,swtich=False):   #处理链接页的数据
        if self.page_xpath:
            one_data_dict = {}
            for key, keyxpath in self.page_xpath.items():
                if key == "url_xpath" or key == "url":
                    content_url = line.xpath(keyxpath)
                    if content_url:
                        endUrl = urljoin(base_url, content_url[0])
                        one_data_dict["url"] = endUrl
                        continue
                    else:  # 没有获取到url
                        swtich = True

                keystr = line.xpath(keyxpath)
                keystr = "".join(keystr)

                if keystr == "images" or keystr == "images_xpath":  # 对图片的链接进行处理
                    keystr = urljoin(base_url, keystr)

                one_data_dict[key] = keystr
            end_data = json.dumps(one_data_dict)  # 将字典转化为字符串

        else:
            end_data = urljoin(base_url,line)
        return end_data,swtich

    def deal_json_page_data(self,base_url,line,swtich=False):
        if self.page_xpath:
            one_data_dict = {}
            swtich = False
            for key, keyxpath in self.page_xpath.items():
                if key == "url_xpath" or key == "url":
                    content_url = jsonpath.jsonpath(line, keyxpath)
                    if content_url:
                        endUrl = urljoin(base_url, content_url[0])
                        one_data_dict["url"] = endUrl
                        continue
                    else:  # 没有获取到url
                        swtich = True

                keystr = jsonpath.jsonpath(line, keyxpath)
                keystr = " ".join(keystr)
                one_data_dict[key] = keystr
            end_data = json.dumps(one_data_dict)  # 将字典转化为字符串
        else:
            end_data = urljoin(base_url, line)
        return end_data,swtich
    # 根据url获取该页面的所有文本的链接或者链接字典

    def judge_url_in_bloom(self,judge_data):
        """判断url或字典里的url是否在布隆过滤器，不在的话加入布隆过滤器,并将数据加入redis"""
        if judge_data.startswith("{"):
            judge_data_json = json.loads(judge_data)
            insert_url = judge_data_json["url"]
            if insert_url in self.bloom:
                return True
            else:
                self.bloom.add(insert_url)
                print(judge_data)
                self.redis.lpush(self.url_key_name, judge_data)
                return False
        else:
            if judge_data in self.bloom:
                return True
            else:
                self.bloom.add(judge_data)
                print(judge_data)
                self.redis.lpush(self.url_key_name, judge_data)
                return False

    def get_content_url_list(self, url):
        """获取静态链接页内容"""
        """获取静态链接页内容"""
        endUrlList = []
        response = self.download(url)
        if response[1] == 200:
            ps = response[0]
            mytree = lxml.etree.HTML(ps)
            linelist = mytree.xpath(self.lineListXpath)
            for line in linelist:
                dealed_page_data, swtich = self.deal_html_page_data(url, line)
                if dealed_page_data and not swtich:  # swtich处理链接页，有一行没有获取到链接的情况
                    endUrlList.append(dealed_page_data)
        return endUrlList


    # json  根据 url获取该  json  页面所有的链接以及其他数据
    def get_json_content_url_list(self, url):
        """获取动态链接页内容"""
        end_data_list = []
        response = self.download(url)
        if response[1] == 200:
            ps = response[0]
            ps = ps.replace("\n", "")
            if self.json_page_re:
                ps = re.compile(self.json_page_re).findall(ps)
                if ps:
                    ps = ps[0]
                else:
                    logging.info(url + "---------这个url用json_page_re处理，结果为空")
                    return
            myjson = json.loads(ps)
            linelist = jsonpath.jsonpath(myjson, self.lineListXpath)
            for line in linelist:
                one_data_dict, swtich = self.deal_json_page_data(url, line)
                if swtich:
                    continue
                end_data_list.append(one_data_dict)
        return end_data_list

    #  post 的有关函数
    #根据url和datapost下载数据
    def post_download(self,url,data):
        try:
            if self.proxy == "1":
                proxy = self.get_proxy().strip()
                proxies = {'https': proxy}  # 获取代理
                response = requests.post(url, proxies=proxies, timeout=self.timeout, headers=self.headers,data=data)
                logging.info(url)
                logging.info("以使用代理")
            else:
                response = requests.post(url, timeout=self.timeout, headers=self.headers,data=data)

            statusCode = response.status_code
            codeStyle = cchardet.detect(response.content)["encoding"]
            if not codeStyle:
                codeStyle = "utf-8"
            webData = response.content.decode(codeStyle, errors="ignore")
            print(webData)
            return (webData, statusCode)
        except Exception as e:
            print(e)
            return (0, 0)

    def get_post_data_list(self):
        data_list = []
        for i in range(int(self.second_page_value), int(self.end_page_value),int(self.page_interval)):
            current_page_data = self.post_data.copy()
            current_page_data[self.page_num_str] = str(i)
            data_list.append(current_page_data)
        return data_list

    def post_html(self,post_data_list):
        switch = False
        for post_data in post_data_list:
            time.sleep(self.timeInterval)
            response = self.post_download(self.start_url, post_data)
            if response[1] == 200:
                ps = response[0]
                mytree = lxml.etree.HTML(ps)
                linelist = mytree.xpath(self.lineListXpath)
                for line in linelist:
                    one_data_dict, swtich_url = self.deal_html_page_data(self.start_url, line)
                    if swtich_url:
                        continue
                    judge_answer = self.judge_url_in_bloom(one_data_dict)
                    if self.executionType != 1 and judge_answer:  # 增量爬虫
                        switch = True

            if switch:  # 布隆过滤器判断有去重
                break


    def post_json(self,post_data_list):
        for post_data in post_data_list:
            swtich = False  # 判断这一页是否有布隆过滤器去重

            time.sleep(self.timeInterval)
            response = self.post_download(self.start_url, post_data)
            if response[1] == 200:
                ps = response[0]
                myjson = json.loads(ps)
                linelist = jsonpath.jsonpath(myjson, self.lineListXpath)
                for line in linelist:
                    # 每一行的操作
                    one_data_dict, swtich_url = self.deal_json_page_data(self.start_url, line)
                    if swtich_url:  # 这一行没有url，跳过这一行
                        continue

                    judge_answer = self.judge_url_in_bloom(one_data_dict)

                    if self.executionType != 1 and judge_answer:  # 增量爬虫
                        swtich = True
            if swtich:
                break

    def get_post_url_list(self):
        """针对wen_type为4，即post的url变化但是post  data不变的情况
        http://www.nhsa.gov.cn/module/web/jpage/dataproxy.jsp?startrecord=%d&endrecord=%p&perpage=15
        """
        end_url_list = []
        for first_num in range(int(self.second_page_value),int(self.end_page_value),int(self.page_interval)):
            second_num = first_num+int(self.page_interval)-1
            if second_num>int(self.end_page_value):
                second_num = int(self.end_page_value)
            post_url = self.start_url.replace("%d",str(first_num)).replace("%p",str(second_num))
            end_url_list.append(post_url)
        return end_url_list

    def post_url_change(self):
        if self.page_xpath:
            switch = False
            url_list = self.get_post_url_list()
            for url in url_list:
                time.sleep(self.timeInterval)
                response = self.post_download(url,self.post_data)
                if response[1] == 200:
                    ps = response[0]
                    mytree = lxml.etree.HTML(ps)
                    linelist = mytree.xpath(self.lineListXpath)
                    for line in linelist:
                        one_data_dict = {}
                        swtich_url = False
                        for key, keyxpath in self.page_xpath.items():
                            if key == "url_xpath" or key == "url":
                                content_url = line.xpath(keyxpath)
                                if content_url:
                                    content_url = content_url[0]
                                    content_url = parse.unquote(content_url)
                                    endUrl = urljoin(self.start_url, content_url)
                                    one_data_dict["url"] = endUrl
                                    continue
                                else:  # 没有获取到url
                                    swtich_url=True

                            keystr = line.xpath(keyxpath)
                            keystr = "".join(keystr)

                            if keystr == "images" or keystr == "images_xpath":  # 对图片的链接进行处理
                                keystr = urljoin(self.start_url, keystr)

                            one_data_dict[key] = keystr
                        if swtich_url:
                            continue
                        bloom_url = one_data_dict["url"]
                        if self.executionType != 1:  # 增量爬虫
                            if bloom_url in self.bloom:
                                logging.info(self.taskCode+"判断url在布隆过滤器成功")
                                switch = True
                            else:
                                self.bloom.add(bloom_url)
                                one_data_dict = json.dumps(one_data_dict)  # 将字典转化为字符串
                                print(one_data_dict)
                                self.redis.lpush(self.url_key_name, one_data_dict)
                        else:
                            one_data_dict = json.dumps(one_data_dict)  # 将字典转化为字符串
                            print(one_data_dict)
                            self.redis.lpush(self.url_key_name, one_data_dict)

                if switch:  # 布隆过滤器判断有去重
                    break
        else:
            swtich = False
            url_list = self.get_post_url_list()
            for url in url_list:
                time.sleep(self.timeInterval)
                response = self.post_download(url,self.post_data)
                if response[1] == 200:
                    ps = response[0]
                    mytree = lxml.etree.HTML(ps)
                    linelist = mytree.xpath(self.lineListXpath)
                    for ii in linelist:
                        content_url = parse.unquote(ii)
                        endUrl = urljoin(self.start_url, content_url)
                        if self.executionType != 1:  # 增量爬虫
                            if endUrl in self.bloom:
                                logging.info(self.taskCode + "判断url在布隆过滤器成功")
                                swtich=True
                            else:
                                self.bloom.add(endUrl)
                                print(endUrl)
                                self.redis.lpush(self.url_key_name, endUrl)
                        else:
                            print(endUrl)
                            self.redis.lpush(self.url_key_name, endUrl)

                if swtich:
                    break


        url_list = self.get_post_url_list()
        for url in url_list:
            response = self.post_download(url,self.post_data)
            if response[0]==200:
                ps = response[1]


    def post_start(self):
        """post_data,page_num_str"""
        if self.webType == 2:  #post，html类型
            post_data_list = self.get_post_data_list()  # 构造post请求数据
            self.post_html(post_data_list)
        elif self.webType == 3: # post  json类型
            post_data_list = self.get_post_data_list()  # 构造post请求数据
            self.post_json(post_data_list)
        else:   #web_type==4,url变化但是postdata不变的情况
            self.post_url_change()

    #html和json的get方法处理
    def get_start(self):

        # 存量爬虫
        if self.executionType == 1:
            pageList = self.get_PageUrlList()  # 页数链接

            for url in pageList:
                time.sleep(self.timeInterval)
                if self.webType == 0:
                    urlList = self.get_content_url_list(url)
                else:
                    urlList = self.get_json_content_url_list(url)
                time.sleep(self.timeInterval)

                for content_data in urlList:
                    print(content_data)
                    self.redis.lpush(self.url_key_name, content_data)
        # 增量爬虫
        else:
            switch = False
            if self.webType == 0:
                start_data_urlList = self.get_content_url_list(self.start_url)
            else:
                start_data_urlList = self.get_json_content_url_list(self.start_url)
            time.sleep(self.timeInterval)

            # 链接页只有url的情况下
            if not self.page_xpath:
                for start_data in start_data_urlList:  # 判断第一页
                    if start_data in self.bloom:
                        logging.info(self.taskCode + "判断url在布隆过滤器成功")
                        switch = True  # 如果第一页出现以前爬过的url，switch为true，后续的就不在爬了
                    else:
                        self.bloom.add(start_data)
                        print(start_data)
                        self.redis.lpush(self.url_key_name, start_data)

                if not switch:  # 判断第二页及以后页数
                    for pageIndex in range(int(self.second_page_value), int(self.end_page_value)):
                        swtich2 = False
                        theUrl = self.url_type.replace("%d", str(pageIndex))

                        if self.webType == 0:
                            second_content_urlList = self.get_content_url_list(theUrl)  # 每一页的文本链接列表
                        else:
                            second_content_urlList = self.get_json_content_url_list(theUrl)  # json格式的每一页的文本链接列表

                        for second_content_url in second_content_urlList:
                            if second_content_url in self.bloom:
                                logging.info(self.taskCode + "判断url在布隆过滤器成功")
                                swtich2 = True
                            else:
                                self.bloom.add(second_content_url)
                                self.redis.lpush(self.url_key_name, second_content_url)
                                print(second_content_url)
                        if swtich2:
                            break
            # 文本链接在一个字典里    {"url": "http://www.nea.gov.cn/2015-01/16/c_133924732.htm","statement_time_xpath":  "2015-01-16"}
            else:
                for start_data in start_data_urlList:  # 判断第一页
                    start_data_json = json.loads(start_data)
                    current_url = start_data_json["url"]
                    if current_url in self.bloom:
                        logging.info(self.taskCode + "判断url在布隆过滤器成功")
                        switch = True  # 如果第一页出现以前爬过的url，switch为true，后续的就不在爬了
                    else:
                        self.bloom.add(current_url)
                        self.redis.lpush(self.url_key_name, start_data)
                        print(start_data)

                if not switch:  # 判断第二页及以后页数
                    for pageIndex in range(int(self.second_page_value), int(self.end_page_value)):
                        swtich2 = False
                        theUrl = self.url_type % pageIndex  # 从第二页开始构造链接

                        if self.webType == 0:
                            second_content_urlList = self.get_content_url_list(theUrl)  # 每一页的文本链接列表
                        else:
                            second_content_urlList = self.get_json_content_url_list(theUrl)  # json格式的每一页的文本链接列表

                        for second_content_data in second_content_urlList:
                            second_content_data_json = json.loads(second_content_data)
                            current_url = second_content_data_json["url"]
                            if current_url in self.bloom:
                                logging.info(self.taskCode + "判断url在布隆过滤器成功")
                                swtich2 = True
                            else:
                                self.bloom.add(current_url)
                                print(current_url)
                                self.redis.lpush(self.url_key_name, second_content_data)
                                print(second_content_data)
                        if swtich2:
                            break
        


    def judge_status(self,task_data):
        """处理周期执行任务，判断周期执行的任务状态，在暂停和停止状态下的处理情况"""

        task_data_json = json.loads(task_data)
        task_code = task_data_json["taskCode"]
        task_key_name = self.redis_platform_address + ":task"   #任务队列键值

        status_key_name = self.redis_platform_address + ":status:" + task_code  # 状态队列键值
        status_data = self.redis.get(status_key_name)
        print("status_key_name",status_key_name)
        print("status_data",status_data)
        status_data = json.loads(status_data)
        status = status_data["status"]

        if status=="1" or status=="2":
            print("判断状态为进行中", task_data)
            self.redis.lrem(task_key_name, 0, task_data)
            print("删除任务", task_data)
            return True
        if status=="3":
            print("判断状态为暂停",task_data)
            time.sleep(1)
            return False
        if status=="4":
            print("判断状态为停止",task_data)
            time.sleep(1)
            self.redis.lrem(task_key_name,0,task_data)
            print("删除任务",task_data)
            return False

    def start(self):
        while True:
            task_key_name = self.redis_platform_address+":task"
            task_data_list = self.redis.lrange(task_key_name,0,100)
            print(task_data_list)
            time.sleep(5)
            for task_data in task_data_list:
                swtich = self.judge_status(task_data)   # 更新self.taskCode

                if swtich:
                    print(self.taskCode)
                    self.taskCode = json.loads(task_data)["taskCode"]
                    self.change_outqueue_num()      #更改outQueue值为1
                    self.update_attr()  # 更新属性

                    if self.executionType != 1:    #增量爬虫      更新布隆过滤器      executionType
                        self.bloom_readfrom_db()

                    if self.post_data or type(self.post_data) == dict:
                        self.post_start()        #处理post
                    else:
                        self.get_start()     #处理get方法html和json

                    if self.executionType != 1:
                        self.bloom_writeto_db()  # 布隆过滤器保存到数据库



if __name__ == "__main__":
    mytest = Main()
    mytest.start()
