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

    ps = requests.get(url).content
    codeStyle = cchardet.detect(ps)["encoding"]
    ps = ps.decode(codeStyle)
    mytree = lxml.etree.HTML(ps)
    endData = {}

    for key,keyxpath in tempData.items():
        if type(keyxpath)==int or  (not keyxpath.startswith(r"//")):
            continue

        if key == "htmlContentXpath": #htmlContentXpath单独处理
            html_content = mytree.xpath(keyxpath)  # html_content

            if html_content:
                html_content = lxml.etree.tostring(html_content[0])
                codeStyle = cchardet.detect(html_content)["encoding"]
                html_content = html_content.decode(codeStyle, errors="ignore")
                html_content = html_content.replace("\n", " ").replace("\t", " ").replace("\r", " ")
                endData["htmlContentXpath"] = html_content
                continue
            else:
                html_content = ""
                endData["htmlContentXpath"] = html_content
                continue

        keystr = mytree.xpath(keyxpath)
        keystr = " ".join(keystr)
        keystr = keystr.replace("\n", " ").replace("\t", " ").replace("\r", " ")
        endData[key] = keystr
        endData["url"] = url

    return endData


def get_one_url(lineListXpath, start_url):
    url = start_url

    response = requests.get(url)
    ps = response.content.decode("utf-8")
    mytree = lxml.etree.HTML(ps)

    lienlist = mytree.xpath(lineListXpath)
    if lienlist:
        lineUrl = lienlist[0]
        endUrl = urljoin(url, lineUrl)
        return endUrl
    else:
        return None



@app.route('/test_template', methods=['POST'])
def hello_world():
    try:
        data = request.get_data()
        data = data.decode("utf-8")
        data = json.loads(data)
        templateInfo_data = data

        start_url = templateInfo_data["start_url"]

        lineListXpath = templateInfo_data["lineListXpath"]

        url = get_one_url(lineListXpath, start_url)
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
    except Exception as e:
        endData = {
    "status":"2",
    "errorDesc":str(e),
    "successDesc":""
}
        return endData


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8951, debug=True)
