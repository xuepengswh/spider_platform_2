from flask import Flask, request,jsonify
import requests
import lxml.etree
import selenium
from bs4 import BeautifulSoup
import re
import json
from urllib.parse import urljoin
import cchardet
import jsonpath

app = Flask(__name__)

def get_data(url, data):    #文本页获取文本数据
    print(url)
    # 提取xpath
    tempData = data["xpaths"]
    if "headers" in tempData:
        headers = tempData["headers"]
    else:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        }
    ps = requests.get(url,headers=headers,verify=False).content
    codeStyle = cchardet.detect(ps).get("encoding","utf-8")
    if not codeStyle:
        codeStyle="utf-8"
    ps = ps.decode(codeStyle,errors="ignore")
    mytree = lxml.etree.HTML(ps)
    endData = {}

    for key,keyxpath in tempData.items():
        if type(keyxpath)==int or  (not keyxpath.startswith(r"//")):
            continue

        if key == "html_content_xpath" or key=="head_info_data": #htmlContentXpath单独处理
            html_content = mytree.xpath(keyxpath)  # html_content

            if html_content:
                html_content = lxml.etree.tostring(html_content[0],encoding="utf-8", pretty_print=True, method="html")
                codeStyle = cchardet.detect(html_content).get("encoding","utf-8")
                if not codeStyle:
                    codeStyle = "utf-8"
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
    print(endData)
    return endData

def get_one_url(line_list_xpath, start_url,url_xpath,headers):    #链接页获取一个文本链接
    url = start_url
    if headers:
        headers = headers
    else:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        }
    ps = requests.get(url, headers=headers,verify=False).content
    codeStyle = cchardet.detect(ps).get("encoding","utf-8")
    if not codeStyle:
        codeStyle="utf-8"
    ps = ps.decode(codeStyle,errors="ignore")
    mytree = lxml.etree.HTML(ps)

    if url_xpath:   #链接页不只是提取链接
        lienlist = mytree.xpath(line_list_xpath)
        line_data = lienlist[0]
        line_url = line_data.xpath(url_xpath)
        if not line_url:
            line_data = lienlist[1]
            line_url = line_data.xpath(url_xpath)
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

def get_dongtai_one_url(line_list_xpath, start_url,url_xpath,json_page_re):    #链接页获取一个文本链接
    url = start_url
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
    }
    response = requests.get(url,headers=headers,verify=False)
    ps = response.content
    codeStyle = cchardet.detect(ps).get("encoding","utf-8")
    if not codeStyle:
        codeStyle="utf-8"
    ps = ps.decode(codeStyle, errors="ignore")
    if json_page_re:
        ps = re.compile(json_page_re).findall(ps)[0]
        myjson = json.loads(ps)
    else:
        myjson = json.loads(ps)

    if url_xpath:   #链接页不只是提取链接
        # lienlist = mytree.xpath(line_list_xpath)
        line_list = jsonpath.jsonpath(myjson,line_list_xpath)
        line_data = line_list[0]
        # line_url = line_data.xpath(url_xpath)
        print(line_data)

        line_url = jsonpath.jsonpath(line_data,url_xpath)
        print(line_url)
        end_url = urljoin(url, line_url[0])
        return end_url

    else:
        line_list = jsonpath.jsonpath(myjson,line_list_xpath)
        if line_list:
            line_url = line_list[0]
            endUrl = urljoin(url, line_url)
            return endUrl
        else:
            return None

def get_post_one_url(line_list_xpath, start_url, url_xpath,post_data,page_num_str,first_page_num):
    url = start_url
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
    }
    post_data[page_num_str] = int(first_page_num)
    # print(post_data)
    ps = requests.post(url, headers=headers,data=post_data).content
    codeStyle = cchardet.detect(ps).get("encoding","utf-8")
    if not codeStyle:
        codeStyle="utf-8"
    ps = ps.decode(codeStyle,errors="ignore")
    mytree = lxml.etree.HTML(ps)
    if url_xpath:
        lienlist = mytree.xpath(line_list_xpath)
        line_data = lienlist[0]
        line_url = line_data.xpath(url_xpath)
        end_url = urljoin(url, line_url[0])
        return end_url
    else:
        content_url_list = mytree.xpath(line_list_xpath)
        print(content_url_list)
        content_url = content_url_list[0]
        content_url = urljoin(start_url,content_url)
        return content_url

def get_json_post_one_url(line_list_xpath, start_url, url_xpath,post_data,page_num_str,first_page_num):
    url = start_url
    headers = {
		"Host":"www.chinatax.gov.cn",
		"Content-Length":"61",
		"Accept":"application/json, text/javascript, */*; q=0.01",
		"X-Requested-With":"XMLHttpRequest",
		"User-Agent":"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
		"Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
		"Origin":"http://www.chinatax.gov.cn",
		"Referer":"http://www.chinatax.gov.cn/chinatax/n810341/n810825/index.html?title=",
		"Accept-Encoding":"gzip, deflate",
		"Accept-Language":"zh-CN,zh;q=0.9",
		"Cookie":"yfx_c_g_u_id_10003701=_ck20081015092014007101321567515; yfx_f_l_v_t_10003701=f_t_1597043360399__r_t_1600411258438__v_t_1600411258438__r_c_5; _Jo0OQK=37AFAD02C9B8FDBF38965B129D738A2069A41BDF4EE142E4E0F1575C056C78452A10B253E9D55898D300CBE5AAF4B9ABF2EEC992AA05909E841B54F7EE1A53A474B34275DAD340EB4DDFFF13AA80B4DD4EFFFF13AA80B4DD4EF78DDF7DE4DEBA64AB39E6D4B415570C1GJ1Z1fQ==; CPS_SESSION=3851AF0CC3C48C245EC8E340B9CE3961",
		"Connection":"keep-alive"
	}
    post_data[page_num_str] = int(first_page_num)
    # print(post_data)
    ps = requests.post(url, headers=headers, data=post_data).content
    codeStyle = cchardet.detect(ps).get("encoding","utf-8")
    if not codeStyle:
        codeStyle="utf-8"
    ps = ps.decode(codeStyle, errors="ignore")
    myjson = json.loads(ps)
    if url_xpath:
        line_list = jsonpath.jsonpath(myjson, line_list_xpath)
        line_data = line_list[0]
        # line_url = line_data.xpath(url_xpath)
        print(line_data)

        line_url = jsonpath.jsonpath(line_data, url_xpath)
        print(line_url)
        end_url = urljoin(url, line_url[0])
        return end_url
    else:
        content_url_list = jsonpath.jsonpath(myjson,line_list_xpath)
        content_url = content_url_list[0]
        content_url = urljoin(start_url, content_url)
        return content_url

@app.route('/test_template', methods=['POST'])
def hello_world():
    data = request.get_data()
    codeStyle = cchardet.detect(data).get("encoding","utf-8")
    if not codeStyle:
        codeStyle="utf-8"
    data = data.decode(codeStyle,errors="ignore")
    data = json.loads(data)
    templateInfo_data = data

    json_page_re = templateInfo_data.get("json_page_re")
    headers = templateInfo_data.get("headers")

    start_url = templateInfo_data["start_url"]
    line_list_xpath = templateInfo_data["line_list_xpath"]
    if "page_xpath" in templateInfo_data:
        url_xpath = templateInfo_data["page_xpath"]["url_xpath"]  # linelist页有需要提起的其他数据
    else:
        url_xpath = False

    print(templateInfo_data["web_type"],"------------------------")
    if templateInfo_data["web_type"] == 0:
        url = get_one_url(line_list_xpath, start_url, url_xpath,headers=headers)
    elif templateInfo_data["web_type"] == 1:
        url = get_dongtai_one_url(line_list_xpath, start_url, url_xpath,json_page_re)


    elif templateInfo_data["web_type"] == 2:
        post_data = templateInfo_data["post"]
        page_num_str = templateInfo_data["page_num_str"]
        first_page_num = templateInfo_data["second_page_value"]
        url = get_post_one_url(line_list_xpath, start_url, url_xpath,post_data,page_num_str,first_page_num)
    else:
        post_data = templateInfo_data["post"]
        page_num_str = templateInfo_data["page_num_str"]
        first_page_num = templateInfo_data["second_page_value"]
        url = get_json_post_one_url(line_list_xpath, start_url, url_xpath,post_data,page_num_str,first_page_num)


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
            "errorDesc": "lineListXpath获取文本链接部分出现错误，即get_one_url或者get_dongtai_url函数返回空，没有返回url",
            "successDesc": ""
        }
    print(endData)
    return endData

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8951, debug=True)
