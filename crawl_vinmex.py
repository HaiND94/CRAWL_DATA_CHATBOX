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
KEYS = ("urls", "type")
data = pd.read_csv('data.csv')

check = all(key in data.keys() for key in KEYS)

if not check:
    print("Please check csv data, miss key")

urls_sources = []
for _data in data.values:
    urls_sources.append(_data)


def get_queue_data(q, url, type):
    url_base = url

    page = requests.get(url_base)

    if page.status_code != 200:
        logger.info(f"Can not connect to page {url_base}")

    soup = BeautifulSoup(page.content, "html.parser")

# Get end page
    data_page = soup.find("span", {"class": "current"})
    sum_page = int(data_page.get_text().split("/")[-1])

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

        # Check connect success
        if page.status_code != 200:
            time.sleep(0.5)
            logger.info(f"Can not connet to {url}")
            url = f"{url_base}?page={page_num}/"

            continue

        # Get urls in number page
        soup = BeautifulSoup(page.content, "html.parser")
        div_data = soup.find_all("div", {"class": "post-list"})
        if not div_data:
            logger.error(f"Can not find div_data for url")
            continue

        # GET a tag data
        a_data = []
        for data in div_data:
            _data = data.find_all("a")
            if _data:
                a_data.append(_data)

        # Get array urls
        urls_tmp = []
        for a_list in a_data:
            for _a in a_list:
                try:
                    _url = _a['href']
                except Exception as e:
                    logger.warning(e)
                    continue
                urls_tmp.append(_url)

        # check duplicates in list urls
        urls = remove_duplicates(urls_tmp)
        

        if not urls:
            logger.error("Can not find data urls")
            continue
        
        # Insert to database
        for _data_url in urls:  
            # PUT data into queue 
            data_queue = {
                "source": url_base, 
                "type": type,
                "url": 'https://www.vinmec.com/vi' + _data_url
            }

            q.put(data_queue)
                    
        
        # Get new page data
        url = f"{url_base}?page={page_num}/"
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
        check_have = mongo_db.find_one({"question": question})
        if check_have:
            logger.info(f"Skip for question {question}")
            continue

    # Get answer data
        # Get content
        div_data = soup.find("div", {"class": "rich-text"})
        content_list = div_data.find_all("p")
        check_text = question.lower()

        list_txts = []
        for p in content_list:
            try:
                list_txts.append(p.get_text().lower())
            except Exception as e:
                logger.warning(e)
                continue

        answer = get_vimex_data(list_txts, check_text)
        if not answer:
            logger.warn(f"Can not get answer for {url}")
            continue

        json_data = {
            "source": source,
            "type": type,
            "url": url,
            "question": question,
            "answer": answer.replace("vinmec", "vfast"),
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
    url = "https://www.vinmec.com/vi/tin-tuc/hoi-dap-bac-si/"
    type = 'general'

    thread_queue = dict()
    for idx, urls in enumerate(urls_sources):
        thread_queue[f'thread_{idx}'] = Thread(target=get_queue_data, args=(queues, urls[0], urls[1]))
        thread_queue[f'thread_{idx}'].start()
        time.sleep(0.1)

    time.sleep(3)

    thread_dict = dict()
   
    for i in range(MAX_THREAD):
        thread_dict[f'thread_{i}'] = Thread(target=get_data, args=(queues, chatbot_table))
        thread_dict[f'thread_{i}'].start()
    
    for i in range(len(urls_sources)):
        thread_queue[f'thread_{i}'].join()

    for i in range(MAX_THREAD):
        thread_dict[f'thread_{i}'].join()







