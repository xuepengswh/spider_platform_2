import configparser
import pymongo
import requests
import time
from urllib.parse import urljoin
import json

configPath = "config.ini"
WebConfig = configparser.ConfigParser()
WebConfig.read(configPath, encoding='utf-8-sig')


upload_url = WebConfig.get("upload_file", "url")    #上传文件的url地址
file_path = WebConfig.get("upload_file", "file_path")   #文件的根路径
group_name = WebConfig.get("upload_file", "group_name") #项目组名   #上传的三个设置



def post_data(file_type):
    """三个参数都是在设置脚本中读取的"""
    data = {
        "groupName":group_name
    }
    end_file_path = file_path+"temp."+file_type
    files = [
        ('multipartFile', open(end_file_path, 'rb')),
    ]

    print(upload_url)
    response = requests.post(upload_url,data=data,files=files).content.decode("utf-8")
    myjson = json.loads(response)
    result = myjson.get("result")
    success = myjson.get("success")
    print("success,result----------",success,result)
    return ( success, result )



def download_data(url,file_type):
    """成功返回200，失败返回False"""
    try:
        response = requests.get(url,verify=False)
    except:
        print(123)
        time.sleep(1)
        return 0
    status_code = response.status_code
    if status_code==200:
        content = response.content
        end_file_path = file_path+"temp."+file_type
        temp_file = open(end_file_path,"wb")
        temp_file.write(content)
        temp_file.close()
        print("status_code-----------",status_code)
        return status_code
    else:
        return False


def get_one_update_data(line_data,field_name):
    """获取链接和路径数据"""
    # 单条数据处理
    content_url = line_data["url"]  # 文本url
    image_url_list = line_data[field_name]  # 链接列表

    # 下载数据，上传数据，构造数据
    end_update_data = []
    for image_url in image_url_list:
        full_image_url = urljoin(content_url, image_url)
        file_type = full_image_url.split(".")[-1]  # 附件类型
        if file_type in ["pdf","doc","docx","png","jpg","gif","xls","xlsx","ppt","pptx"]:
            judge_sueccess = download_data(full_image_url, file_type)  # 下载数据
            if judge_sueccess:
                success, result = post_data(file_type)  # 上传
                if success:
                    url_and_path = (image_url, result)  # 构造数据
                    end_update_data.append(url_and_path)
    return end_update_data



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

    upload_url = WebConfig.get("upload_file", "url")    #上传文件的url地址
    file_path = WebConfig.get("upload_file", "file_path")   #文件的根路径
    group_name = WebConfig.get("upload_file", "group_name") #项目组名   #上传的三个设置

    # myMongo = connect_mongo()  # 链接mongodbdb数据库

    # task_code = "1b338938"  #数据库的task_code
    # field_name = "images_source"    #链接的字段名

    # go(task_code,field_name)
