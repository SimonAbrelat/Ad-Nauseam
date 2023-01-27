from threading import Lock
from multiprocessing import Queue, pool
import dataclasses 
from collections import deque
import hashlib
import time

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
    crawl_time: float 
    timestamp: float 

    def __init__(self, num_urls, num_words, elapsed_time, stamp):
        self.urls_extracted = num_urls
        self.keywords_extracted = num_words
        self.crawl_time = elapsed_time
        self.timestamp= stamp

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
    file_lock = Lock()

    def __init__(self, seed, db_file, perf_file):
        self.db = TinyDB(db_file)
        self.expored_filter = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH)
        self.frontier_urls = Queue()
        self.frontier_urls.put(seed)
        self.perf_record = perf_file
        self.start_time = time.time()
        with open(self.perf_record, "w") as mon:
            mon.write("Successfully Crawled, Timestamp (us), New URLs Added, New Keywords Extracted, Individual Crawl Time (us), " +\
                "10 Crawl Rolling Ave (us), Pages Traversed / Min, Total Keywords Extracted, Keywords / Min, Attempted Crawl, Total URLs, Success Rate, Success Ratio\n")

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
        self.rolling_average.append(perf.crawl_time)
        rolling = sum(self.rolling_average) / len(self.rolling_average)

        with self.file_lock:
            with open(self.perf_record, "a") as mon:
                mon.write(
                    f"{self.successful_urls}, {perf.timestamp}," + \
                        f"{perf.urls_extracted},{perf.keywords_extracted},{perf.crawl_time},{rolling}," + \
                        f"{60 / rolling},{self.total_keywords},{(self.total_keywords * 60)/(perf.timestamp)},{self.attempted_urls},{self.total_urls}," + \
                        f"{self.successful_urls / self.attempted_urls},{self.successful_urls / self.total_urls}\n"
                )

    def run(self):
        self.start_time = time.time()
        with pool.ThreadPool(processes=10) as t_p:
            while(self.successful_urls < 10000):
                t_p.apply(crawl)

manager = CrawlerManager(base_url + "/wiki/Embedded_C%2B%2B", "db.json", "perf.csv")

def crawl():
    global manager
    start = time.time()
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
    elapsed_time = time.time() - start
    timestamp = (time.time() - manager.start_time )
    # Record Data
    perf = PerfMon(num_urls, len(keywords_ext), elapsed_time, timestamp)
    data = CrawlData(url, mac, keywords_ext)
    manager.record(data, perf)

manager.run()