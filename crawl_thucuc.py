import pandas as pd
import requests
import configparser
import time
import logging

from pymongo import MongoClient

# from multiprocessing import Process, Queue
from queue import Queue
from threading import Thread

from bs4 import BeautifulSoup

from utils import remove_duplicates, logger
from utils import get_data as get_vimex_data


# Config logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("log_file.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


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

# Get source from config file
# KEYS = ("urls", "type")
# data = pd.read_csv('data.csv')

# check = all(key in data.keys() for key in KEYS)

# if not check:
#     print("Please check csv data, miss key")

# urls_sources = []
# for _data in data.values:
#     urls_sources.append(_data)


def get_queue_data(q, url, type):
    url_base = url

    page = requests.get(url_base)

    if page.status_code != 200:
        logger.info(f"Can not connect to page {url_base}")

    soup = BeautifulSoup(page.content, "html.parser")

# Get end page
   # Get end page
    data_page = soup.find_all("a", {"class": "page-numbers"})
    print(data_page)

    sum_page = int(data_page[1].get_text())

    page_num = 0

# Get ulr of articles
    while page_num <= sum_page:
        page_num += 1
        if page_num == 1:
            pass
        else:
            url = f"{url_base}page/{page_num}/"

        logger.info(f"Get data for {url}")
        try:
            page = requests.get(url=url)
        except Exception as e:
            logger.error(url)
            logger.error(e)
            # https://benhvienthucuc.vn/hoi-dap-chuyen-gia/page/2/
            # url = f"{url_base}page/{page_num}/"
            continue

        # Check connect success
        if page.status_code != 200:
            time.sleep(0.5)
            logger.info(f"Can not connet to {url}")
            # url = f"{url_base}page/{page_num}/"
            continue

        # Get urls in number page
        soup = BeautifulSoup(page.content, "html.parser")
        data_link = soup.find("div", {"class": "sec32_col1_col2"})
        if not data_link:
            logger.error(f"Can not find div_data for url")
            continue

        list_h2 = data_link.find_all("h2", {"class": "entry-cau-hoi__h1"})
        if not list_h2:
            logger.error(f"Can not find div_data for url")
            continue   

        # GET a tag data
        a_data = []
        for h2 in list_h2:
            try:
                _data = h2.find("a")['href']
            except Exception as e:
                logger.warning("Can not find a tag in h2 tag")
                continue
            if _data:
                a_data.append(_data)


        # check duplicates in list urls
        urls = remove_duplicates(a_data)
        

        if not urls:
            logger.error("Can not find data urls")
            continue
        
        # Insert to database
        for _data_url in urls:  
            # PUT data into queue 
            data_queue = {
                "source": url_base, 
                "type": type,
                "url": _data_url
            }

            q.put(data_queue)
                    
        
        # Get new page data
        time.sleep(0.2)
    
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

        check_have = mongo_db.count_documents({"url": url})
        if check_have:
            logger.info(f"Skip {url}")
            continue

        try:
            page = requests.get(url)
        except Exception as e:
            logger.error(url)
            logger.error(e)
            continue

        if page.status_code != 200:
            logger.info(f"Can not connect to page {url}")
            continue

        soup = BeautifulSoup(page.content, "html.parser")

    # Get question in url
        _question = soup.find('title').get_text()
        if "?" not in _question:
            logger.warning(f"url {url} was skip")
            continue

        if not _question:
            logger.error(f"Can not find title in url {url}")
            continue

        question = _question.split("|")[0][:-1]
        del _question

        logger.info(question)

    # Get answer data
        # Get content
        div_data = soup.find_all("div", {"class": "entry-cau-hoi__content"})
        # print(div_data)
        list_text = []
        for div in div_data:
            if not div:
                continue
            p_data = div.find_all("p")
            # print(p_data)
            for idx, p in enumerate(p_data):
                if idx < 2:
                    continue

                list_text.append(p.get_text())

        answer = "/n".join(list_text)
        if not answer:
            logger.warn(f"Can not get answer for {url}")
            continue

        json_data = {
            "source": source,
            "type": type,
            "url": url,
            "question": question,
            "answer": answer,
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
        
        time.sleep(0.2)


if __name__ == "__main__":
    # Define queue
    queues = Queue(maxsize=30000) 

    # Config page to crawl
    url = "https://benhvienthucuc.vn/hoi-dap-chuyen-gia/"
    type = 'general'

    thread_queue = dict()
    thread_queue = Thread(target=get_queue_data, args=(queues, url, type))
    thread_queue.start()

    time.sleep(3)

    thread_dict = dict()
   
    for i in range(MAX_THREAD):
        thread_dict[f'thread_{i}'] = Thread(target=get_data, args=(queues, chatbot_table))
        thread_dict[f'thread_{i}'].start()
    
    thread_queue.join()


    for i in range(MAX_THREAD):
        thread_dict[f'thread_{i}'].join()







