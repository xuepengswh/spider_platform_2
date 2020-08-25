import json
import time
import redis
import pymongo
import configparser

def connect_mongo():
    mongourl = "mongodb://" + mongoUser + ":" + mongoPassword + "@" + mongoHost + ":" + mongoPort
    conn = pymongo.MongoClient(mongourl)
    db = conn[mongoDb]#数据库名
    return db

def connect_db1():#############################################################################################

    while True:
        dataNum = myredis.llen(data_key_name)
        if dataNum>10:
            for i in range(10):
                oneData = myredis.lpop(data_key_name)
                oneData = json.loads(oneData)
                print(oneData)
                mymongo_db["policy_content"].insert_one(oneData)
        else:
            time.sleep(10)

def connect_db():
    while True:
        dataNum = myredis.llen(data_key_name)
        if dataNum>10:
            for i in range(10):
                oneData = myredis.lpop(data_key_name)
                oneData = json.loads(oneData)
                print(oneData)
                mymongo_db["policy_content"].insert_one(oneData)
        else:
            time.sleep(10)

if __name__=="__main__":
    # 读取配置文件
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
    mongoDb = WebConfig.get("mongodb", "database")

    #连接数据库
    myredis = redis.Redis(host=redisHost, port=redisPort, decode_responses=True, password=redisPassword, db=redisDb)
    data_key_name = redis_platform_address + ":temporary"
    mymongo_db = connect_mongo()

    #运行程序
    connect_db()
    # connect_mongo()
