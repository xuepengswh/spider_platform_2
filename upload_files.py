import requests
import configparser
import pymongo
import requests
import time

def post_data(url,file_path,group_name):
    data = {
        "groupName":group_name
    }
    files = [
        ('multipartFile', open(file_path, 'rb')),
    ]
    ps = requests.post(url,data=data,files=files).content.decode("utf-8")
    print(ps)

def connect_mongo():
    mongourl = "mongodb://"+mongoUser+":"+mongoPassword+"@"+mongoHost+":"+mongoPort
    conn = pymongo.MongoClient(mongourl)
    db = conn[mongoDatabase]#数据库名
    return db

def get_url_from_mongo(task_code,field_name):
    data_list = myMongo[task_code].find({})
    url_list = []
    for data in data_list:
        url = data[field_name]
        url_list.append(url)
    return url_list

def download_data(url):
    try:
        response = requests.get(url,verify=False)
    except:
        print(123)
        time.sleep(1)
        return 0
    status_code = response.status_code
    if status_code==200:
        content = response.content
        temp_file = open(file_path,"wb")
        temp_file.write(content)
        temp_file.close()
        return status_code
    else:
        return False

if __name__=="__main__":
    # 读取配置文件
    configPath = "config.ini"
    WebConfig = configparser.ConfigParser()
    WebConfig.read(configPath, encoding='utf-8-sig')
    mongoHost = WebConfig.get("mongodb", "host")
    mongoPort = WebConfig.get("mongodb", "port")
    mongoUser = WebConfig.get("mongodb", "user")
    mongoPassword = WebConfig.get("mongodb", "password")
    mongoDatabase = WebConfig.get("mongodb", "database")
    upload_url = WebConfig.get("upload_file", "url")
    file_path = WebConfig.get("upload_file", "file_path")
    group_name = WebConfig.get("upload_file", "group_name")

    myMongo = connect_mongo()  # 链接mongodbdb数据库

    task_code = "a748cebb"
    field_name = "image"
    file_form = "jpg"

    file_path = file_path+"temp."+file_form

    file_url_list = get_url_from_mongo(task_code,field_name)
    for file_url in file_url_list:
        print(file_url)
        status_code = download_data(file_url)
        print(status_code)
        if status_code == 200:
            re_data = post_data(upload_url,file_path,group_name)
            print(re_data)