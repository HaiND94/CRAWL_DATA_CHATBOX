import pandas as pd
import requests
import configparser
import time
import re
import pymongo
import logging

from pymongo import MongoClient

# from multiprocessing import Process, Queue
from queue import Queue
from threading import Thread

from bs4 import BeautifulSoup

from utils import remove_duplicates, logger


# Read config file to use for mongodb config
config = configparser.ConfigParser()
config.read("config.cfg")
mongo_config = config['CONFIG_MONGO']
MONGO_URL = mongo_config.get('URL_MONGO')
PORT = mongo_config.get('PORT')
PASS = mongo_config.get("PASS")

chatbot_db = MongoClient(f'mongodb://{MONGO_URL}:{PORT}/')

db = chatbot_db['chatbox']
chatbot_table = db.health_chatbox

# Get multi thread
thread_config = config['CONFIG_THREAD']
MAX_THREAD = int(thread_config.get("NUM"))


# Connect to elasticsearch

# KEYS = ("urls", "type")


# data = pd.read_csv('data.csv')

# check = all(key in data.keys() for key in KEYS)

# if not check:
#     print("Please check csv data, miss key")


# urls_ob = []
# for _data in data.values:
#     urls_ob.append(_data)


def get_queue_data(q, url, type):
    url_base = url

    page = requests.get(url)

    if page.status_code != 200:
        logger.info(f"Can not connect to page {url}")

    soup = BeautifulSoup(page.content, "html.parser")

# Get end page
    list_pages = soup.find_all("a", {"class": "page-link"})
    page =  list_pages[-1]
    # print(page.text.lower())
    href = str(page['href'])
    # print(href)
    sum_page = int(href.split("/")[-2].split("-")[-1])
    page_num = 1

# Get ulr of articles
    while page_num <= sum_page:
        page_num += 1
        logger.info(f"Get data for {url}")
        try:
            page = requests.get(url=url)
        except Exception as e:
            logger.error(url)
            logger.error(e)
            url = f"{url_base}p-{page_num}/"
            continue

        if page.status_code != 200:
            logger.info(f"Can not connet to {url}")
            url = f"{url_base}p-{page_num}/"
            continue

    # Get urls in number page
        soup = BeautifulSoup(page.content, "html.parser")
        data_uls = soup.find_all("div", {"class": "media-body"})
        if not data_uls:
            logger.error("Can not find data urls")
            continue
        for _data in data_uls:
            _urls = _data.find_all("a", href=True)
            for _url in _urls:
                if len(_url) != 0:
                    garbage = [
                                "https://alobacsi.com/thong-tin-bac-si/alobacsi-tra-loi/11", 
                                "https://alobacsi.com/hoi-dap-dich-vu-y-te/bv-phong-kham-san-phu-khoa-hd224/",
                                "https://alobacsi.com/hoi-dap-dich-vu-y-te/covid-19-hd309/", 
                                "https://alobacsi.com/hoi-dap-dich-vu-y-te/bv-phong-kham-nam-khoa-hd226/", 
                                "https://alobacsi.com/hoi-dap-dich-vu-y-te/bv-trung-tam-chan-thuong-chinh-hinh-hd232/", 
                                "https://alobacsi.com/hoi-dap-dich-vu-y-te/bv-phong-kham-san-phu-khoa-hd224/",
                                "https://alobacsi.com/hoi-dap-dich-vu-y-te/bv-phong-kham-da-khoa-hd221/",
                                "https://alobacsi.com/hoi-dap-dich-vu-y-te/trung-tam-xet-nghiem-hd240/",
                                "https://alobacsi.com/hoi-dap-dich-vu-y-te/bv-phong-kham-da-khoa-hd221/",
                                "https://alobacsi.com/hoi-dap-dich-vu-y-te/chich-ngua-vac-xin-hd262/",
                                "https://alobacsi.com/hoi-dap-dich-vu-y-te/ngoai-khoa-hd310/",
                                "https://alobacsi.com/hoi-dap-dich-vu-y-te/bv-phong-kham-da-khoa-hd221/",
                                "https://alobacsi.com/hoi-dap-dich-vu-y-te/bv-phong-kham-chuyen-khoa-than-tiet-nieu-hd259/",
                                "https://alobacsi.com/hoi-dap-dich-vu-y-te/bv-phong-kham-ngoai-than-kinh-phau-thuat-than-kinh-hd249/"
                                ]

                    if _url['href'] in garbage:
                        continue

                    if "bv-phong-kham" in _url['href']:
                        continue
                    
                    # PUT data into queue 
                    data_queue = {
                        "source": url_base, 
                        "type": type,
                        "url": _url['href']
                    }

                    q.put(data_queue)
                    
        
    # Get new page data
        url = f"{url_base}p-{page_num}/"
    
    return True


def get_data(q, mongo_db):
    while True:
        count = 0
        while q.empty() and count <= 10:
            time.sleep(5)
            count += 1

        if q.empty() and count > 10:
            logger.info("Finish")
            break
    
        # Get data from queue
        data_queue = q.get()
        url = data_queue['url']
        source = data_queue['source']
        type = data_queue['type']

        page = requests.get(url)

        if page.status_code != 200:
            logger.info(f"Can not connect to page {url}")
            continue

        soup = BeautifulSoup(page.content, "html.parser")

        # Get urls in number page
        question = soup.find('title').get_text()
        if not question:
            logger.error(f"Can not find title in url {url}")
            continue

        logger.info(question)
        answer_data = soup.find("div", {"class": "main-detail mb-3"})
        if not answer_data:
            logger.error(f"Can not find div in url {url}")
            continue
        
        data = answer_data.find_all("p")
        if not data:
            logger.error(f"Can not find p in div tag in {url}")
            continue

        if len(data) < 2:
            logger.error(f"Search result for {url} not True")
            continue

        answer = ""
        for data in data[1:]:
            answer += f"\n{data.get_text()}"

        json_data = {
            "source": source,
            "type": type,
            "url": url,
            "question": question,
            "answer": answer.replace("AloBacsi", "vfast"),
            "is_check": None,
            "result": False,
            "editter": None,
            "have_trained": False
        }

        # Insert data into database
        try:
            mongo_db.insert_one(json_data)
        except Exception as e:
            logger.error(e)
        
        time.sleep(0.1)


if __name__ == "__main__":
    # Define queue
    queues = Queue(maxsize=30000) 
    

    # Config page to crawl
    url = "https://benhvienthucuc.vn/hoi-dap-chuyen-gia/"
    type = 'general'

    thread_queue = Thread(target=get_queue_data, args=(queues, url, type))
    thread_queue.start()

    time.sleep(3)

    thread_dict = dict()
    
    for i in range(MAX_THREAD):
        thread_dict[f'thread_{i}'] = Thread(target=get_data, args=(queues, chatbot_table))
        thread_dict[f'thread_{i}'].start()
    
    thread_queue.join()
    thread_dict[f'thread_{i}'].join()







