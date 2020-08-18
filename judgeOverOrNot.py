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
import pymongo
from bson.objectid import ObjectId
import configparser

"""
redis状态轮询
    查找出队列的状态outQueue为1
        根据taskCode查找url是否为空
        为空，
            outQueue更新为0，状态更新为已完成
    continue
    轮询一次暂停10秒
"""
def connect_mongo():
    mongourl = "mongodb://"+mongoUser+":"+mongoPassword+"@"+mongoHost+":"+mongoPort
    conn = pymongo.MongoClient(mongourl)
    db = conn.mongoDatabase#数据库名
    return db

def change_redis_status(redis_keyname,dict_key_name,newvalue):
    #redis_keyname   状态键值
    #   dict_key_name   "outQueue"
    #   newvalue    更新后的outQueue值0

    #   更新redis
    myredis = redis.Redis(host=redisHost, port=redisPort, decode_responses=True, password=redisPassword, db=redisDb)
    keyname_data = myredis.get(redis_keyname)   #获取状态数据
    keyname_data = json.loads(keyname_data)     #换为json数据
    keyname_data[dict_key_name] = newvalue      #更新数据，将outQueue更新为0

    executionType = keyname_data["executionType"]   #获取executionType值
    mongo_id = keyname_data["id"]   #获取id值

    keyname_data = json.dumps(keyname_data) #转化为字符串
    myredis.set(redis_keyname,keyname_data) #更新redis


    #   更新mongodb
    if executionType=="1":  #单次执行，改为完成状态，更改mongodb状态
        myMongo["task_info"].update_one({"_id":ObjectId(mongo_id)},{"$set":{"status":"5"}}) #更新数据
    else:   #多次执行，改为未开始
        myMongo["task_info"].update_one({"_id": ObjectId(mongo_id)}, {"$set": {"status": "1"}})

def change_mongo_status():
    pass

def connect_db( ):
    redis_key_list = myredis.keys(redis_platform_address+":status:*")
    while True:
        for key_name in redis_key_list:
            task_code = key_name.split(":")[-1]  # 更新self.task_code

            keyName = redis_platform_address+":status:" +task_code
            status_data = myredis.get(keyName)  # 获得所有状态
            status_data = json.loads(status_data)
            outQueue  = int(status_data["outQueue"])
            if outQueue==1:
                url_key_name = redis_platform_address+":url:"+task_code
                num = myredis.llen(url_key_name)
                if num==0:
                    #   更改  redis状态和mongodb状态
                    change_redis_status(key_name, "outQueue", 0)
            else:
                continue
            time.sleep(3)

if __name__=="__main__":
    #读取配置文件
    configPath = "config.ini"
    WebConfig = configparser.ConfigParser()
    WebConfig.read(configPath, encoding='utf-8-sig')
    redisHost = WebConfig.get("redis", "host")
    redisPort = WebConfig.get("redis", "port")
    redisPassword = WebConfig.get("redis", "password")
    redisDb = WebConfig.get("redis", "database")
    redis_platform_address = WebConfig.get("redis", "redis_platform_address")

    mongoHost = WebConfig.get("mongodb", "host")
    mongoPort = WebConfig.get("mongodb", "port")
    mongoUser = WebConfig.get("mongodb", "user")
    mongoPassword = WebConfig.get("mongodb", "password")
    mongoDatabase = WebConfig.get("mongodb", "database")

    myMongo = connect_mongo()  # 链接mongodbdb数据库
    myredis = redis.Redis(host=redisHost, port=redisPort, decode_responses=True, password=redisPassword, db=redisDb)

    # key_name = redis_platform_address+":status:e7550030"
    # change_redis_status(key_name,"outQueue",0)
    connect_db()
