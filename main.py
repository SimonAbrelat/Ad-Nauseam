from threading import Lock
from multiprocessing import Queue, pool
import dataclasses 
from datetime import datetime
from collections import deque
import hashlib

from tinydb import TinyDB
from bs4 import BeautifulSoup
import lxml
import requests
from pybloom_live import ScalableBloomFilter 

base_url = "https://en.wikipedia.org"

@dataclasses.dataclass
class PerfMon:
    urls_extracted : int
    keywords_extracted : int
    time_ms : int

    def __init__(self, num_urls, num_words, time):
        self.urls_extracted = num_urls
        self.keywords_extracted = num_words
        self.time_ms = time

@dataclasses.dataclass
class CrawlData:
    url : str
    mac : str
    keywords : list[str]


class CrawlerManager:
    total_urls = 0
    attempted_urls = 0
    successful_urls = 0
    total_keywords = 0

    rolling_average = deque(maxlen=10)
    
    filter_lock = Lock()
    db_lock = Lock()

    def __init__(self, seed, db_file, perf_file):
        self.db = TinyDB(db_file)
        self.expored_filter = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH)
        self.frontier_urls = Queue()
        self.frontier_urls.put(seed)
        self.perf_record = perf_file
        with open(self.perf_record, "w") as mon:
            mon.write("New URLs Crawled, New keywords extracted, Recent Crawl Time (ms), 10 Crawl Rolling Ave (ms), Pages / Min, Successfully Crawled, Attempted Crawl, Total URLs, Success Rate, Attempted Ratio, Success Ratio\n")

    def __del__(self):
        self.perf_record.close()

    def add(self, url):
        with self.filter_lock:
            if url in self.expored_filter:
                return
        self.total_urls += 1
        self.frontier_urls.put(url)

    def pop(self):
        while True:
            self.attempted_urls += 1
            ret = self.frontier_urls.get()
            self.filter_lock.acquire()
            if ret in self.expored_filter:
                self.filter_lock.release()
                continue
            self.filter_lock.release()
            break
        return ret 
    
    def record(self, data, perf):
        with self.db_lock:
            self.db.insert(dataclasses.asdict(data))
            self.successful_urls += 1
        with self.filter_lock:
            self.expored_filter.add(data.url)

        self.total_keywords += perf.keywords_extracted
        self.rolling_average.append(perf.time_ms)
        rolling = sum(self.rolling_average) / len(self.rolling_average)

        with open(self.perf_record, "a") as mon:
            mon.write(
                f"{perf.urls_extracted},{perf.keywords_extracted},{perf.time_ms},{rolling},{60000 / rolling}," + \
                f"{self.successful_urls},{self.attempted_urls},{self.total_urls}," + \
                f"{self.successful_urls / self.attempted_urls},{self.successful_urls / self.total_urls},{self.attempted_urls / self.total_urls}\n"
            )

    def run(self):
        with pool.ThreadPool(processes=20) as t_p:
            while True:
                t_p.apply(crawl)

manager = CrawlerManager(base_url + "/wiki/Embedded_C%2B%2B", "db.json", "perf.csv")

def crawl():
    global manager
    print("NEW CRAWLER")
    start = datetime.now()
    # Get the URL
    url = manager.pop()
    try:
        page = requests.get(url)
    except:
        print("ERROR:", url)
        return
    soup = BeautifulSoup(page.content, 'lxml')
    # Parse for new external links
    urls = soup.find_all('a', href=True)
    num_urls = 0
    for a in urls:
        curr = a['href']
        if len(curr) > 1 and curr[0] == '/':
            manager.add(base_url + curr)
            num_urls += 1
    # Generate MAC to record if a website has changed in the future
    mac = hashlib.md5(str(page.content).encode('utf-8')).hexdigest()
    # Get keywords for the webpage
    keywords_ext = [l.text for d in soup.find_all("div", {"id": "mw-normal-catlinks"}) for l in d.find_all('li')]
    # Time Data
    time_ms = (start - datetime.now()).microseconds
    # Record Data
    perf = PerfMon(num_urls, len(keywords_ext), time_ms)
    data = CrawlData(url, mac, keywords_ext)
    manager.record(data, perf)

manager.run()
# cd Documents\School\Year_4\CS4675\Ad-Nauseam