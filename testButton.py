from flask import Flask, request,jsonify
import requests
import lxml.etree
import selenium
from bs4 import BeautifulSoup
import re
import json
from urllib.parse import urljoin
import cchardet


app = Flask(__name__)

def get_data(url, data):
    # 提取xpath
    tempData = data["xpaths"]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
    }
    ps = requests.get(url,headers=headers).content
    codeStyle = cchardet.detect(ps)["encoding"]
    ps = ps.decode(codeStyle)
    mytree = lxml.etree.HTML(ps)
    endData = {}

    for key,keyxpath in tempData.items():
        if type(keyxpath)==int or  (not keyxpath.startswith(r"//")):
            continue

        if key == "html_content_xpath": #htmlContentXpath单独处理
            html_content = mytree.xpath(keyxpath)  # html_content

            if html_content:
                html_content = lxml.etree.tostring(html_content[0],encoding="utf-8", pretty_print=True, method="html")
                codeStyle = cchardet.detect(html_content)["encoding"]
                html_content = html_content.decode(codeStyle, errors="ignore")
                html_content = html_content.replace("\n", " ").replace("\t", " ").replace("\r", " ")
                endData["html_content_xpath"] = html_content
                continue
            else:
                html_content = ""
                endData["html_content_xpath"] = html_content
                continue

        keystr = mytree.xpath(keyxpath)
        keystr = " ".join(keystr)
        keystr = keystr.replace("\n", " ").replace("\t", " ").replace("\r", " ")
        endData[key] = keystr
        endData["url"] = url

    return endData



def get_one_url(line_list_xpath, start_url,url_xpath):    #获取一个文本链接

    url = start_url
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
    }
    response = requests.get(url,headers=headers)
    ps = response.content.decode("utf-8")
    mytree = lxml.etree.HTML(ps)

    if url_xpath:
        lienlist = mytree.xpath(line_list_xpath)
        print(len(lienlist))
        line_data = lienlist[0]
        print(url_xpath)
        line_url = line_data.xpath(url_xpath)
        print(line_url)
        end_url = urljoin(url, line_url[0])
        return end_url

    else:
        lienlist = mytree.xpath(line_list_xpath)
        if lienlist:
            line_url = lienlist[0]
            endUrl = urljoin(url, line_url)
            return endUrl
        else:
            return None


@app.route('/test_template', methods=['POST'])
def hello_world():
    data = request.get_data()
    data = data.decode("utf-8")
    data = json.loads(data)
    templateInfo_data = data

    start_url = templateInfo_data["start_url"]

    line_list_xpath = templateInfo_data["line_list_xpath"]

    if "page_xpath" in templateInfo_data:
        url_xpath = templateInfo_data["page_xpath"]["url_xpath"]  # linelist页有需要提起的其他数据
    else:
        url_xpath = False

    print(line_list_xpath)

    url = get_one_url(line_list_xpath, start_url, url_xpath)
    if url:
        end_content = get_data(url, templateInfo_data)
        endData = {
            "status": "1",
            "errorDesc": "",
            "successDesc": end_content
        }
    else:
        endData = {
            "status": "2",
            "errorDesc": "lineListXpath获取文本链接部分出现错误",
            "successDesc": ""
        }
    return endData


#     try:
#         data = request.get_data()
#         data = data.decode("utf-8")
#         data = json.loads(data)
#         templateInfo_data = data
#
#         start_url = templateInfo_data["start_url"]
#
#         line_list_xpath = templateInfo_data["line_list_xpath"]
#
#         if "page_xpath" in templateInfo_data:
#             url_xpath = templateInfo_data["page_xpath"]["url_xpath"]  # linelist页有需要提起的其他数据
#         else:
#             url_xpath = False
#
#         print(line_list_xpath)
#
#         url = get_one_url(line_list_xpath, start_url, url_xpath)
#         if url:
#             end_content = get_data(url, templateInfo_data)
#             endData = {
#                 "status": "1",
#                 "errorDesc": "",
#                 "successDesc": end_content
#             }
#         else:
#             endData = {
#                 "status": "2",
#                 "errorDesc": "lineListXpath获取文本链接部分出现错误",
#                 "successDesc": ""
#             }
#         return endData
#     except Exception as e:
#         endData = {
#     "status":"2",
#     "errorDesc":str(e),
#     "successDesc":""
# }
#         return endData


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8951, debug=True)
