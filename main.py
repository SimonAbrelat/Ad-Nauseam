from threading import Lock
from multiprocessing import Queue, pool
from tinydb import TinyDB

from bs4 import BeautifulSoup
import lxml
import requests

import hashlib
from pybloom_live import ScalableBloomFilter 

base_url = "https://en.wikipedia.org"

class CrawlerManager:
    total_urls = 0
    attempted_urls = 0
    successful_urls = 0
    
    filter_lock = Lock()
    db_lock = Lock()

    def __init__(self, seed, db_file):
        self.db = TinyDB(db_file)
        self.expored_filter = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH)
        self.frontier_urls = Queue()
        self.frontier_urls.put(seed)

    def add(self, url):
        with self.filter_lock:
            if url in self.expored_filter:
                return
        self.total_urls += 1
        self.frontier_urls.put(url)

    def pop(self):
        ret = self.frontier_urls.get()
        while True:
            self.filter_lock.acquire()
            if ret in self.expored_filter:
                self.filter_lock.release()
                continue
            self.filter_lock.release()
            break
        return ret 
    
    def record(self, url, mac_val, keywords):
        with self.db_lock:
            self.db.insert({
                "url": url, 
                "hash": mac_val,
                "keywords": keywords
            })
            self.successful_urls += 1
            print(self.succesful_urls, "/", self.total_urls)
        with self.filter_lock:
            self.expored_filter.add(url)

    def run(self):
        t_p = pool.ThreadPool(processes=10)
        while(self.successful_urls < 1000):
            t_p.apply(crawl)

manager = CrawlerManager(base_url + "/wiki/Embedded_C%2B%2B", "db.json")

def crawl():
    global manager
    print("NEW CRAWLER")
    # Get the URL
    url = manager.pop()
    try:
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'lxml')
        # Parse for new external links
        urls = soup.find_all('a', href=True)
        for a in urls:
            curr = a['href']
            if curr[0] == '/':
                manager.add(base_url + curr)

        # Get keywords for the webpage
        mac = hashlib.md5(str(page.content).encode('utf-8')).hexdigest()
        manager.record(url, mac, [l.text for d in soup.find_all("div", {"id": "mw-normal-catlinks"}) for l in d.find_all('li')])
        return
    except:
        print("ERROR: ", url)

manager.run()