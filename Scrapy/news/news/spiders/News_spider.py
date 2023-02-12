import scrapy
import json
import requests
import datetime
from Financial_data_crawler.db.clients import MongoClient
from Financial_data_crawler.db.NewsModel import Cynes_News
from Financial_data_crawler.DataCleaner import news_cleaner


def get_urls(start_date=0, end_date=0):
    urls = []
    if not start_date == end_date == 0:
        start = datetime.datetime.strptime(start_date, "%Y-%m-%d").timestamp()
        end = datetime.datetime.strptime(end_date, "%Y-%m-%d").timestamp()
    else:
        start = datetime.datetime.today().timestamp()
        end = start - 86400

    while start > end:
        # Fetch one-month data per time
        if not end == 0:
            old = start - 2592000
        else:
            old = start - 86400

        linkage = f"https://news.cnyes.com/api/v3/news/category/tw_stock?startAt={int(round(old,0))}&endAt={int(round(start,0))}&limit=30&page={1}"
        raw_text = json.loads(requests.get(linkage).text)
        last_page = raw_text["items"]["last_page"]

        for i in range(1, last_page + 1):
            linkage = f"https://news.cnyes.com/api/v3/news/category/tw_stock?startAt={int(round(old,0))}&endAt={int(round(start,0))}&limit=30&page={i}"
            raw_text = json.loads(requests.get(linkage).text)

            for newsid in raw_text["items"]["data"]:
                url = f"https://news.cnyes.com/news/id/{newsid['newsId']}?exp=a"
                if not url in urls:
                    urls.append(url)

        start -= 2592000

    return urls


class NewsSpider(scrapy.Spider):
    name = "News"
    allowed_domains = ["https://news.cnyes.com/"]

    def __init__(self, start_date=0, end_date=0):
        self.start_urls = get_urls(start_date, end_date)
        self.newsdb = MongoClient("Scrapy", "News")
        self.tag_cleaner = news_cleaner.tags_extract()
        self.__docs = Cynes_News

    def parse(self, response):
        headline = response.xpath('//h1[@itemprop="headline"]/text()').get()
        tags = response.xpath('//span[@class="_1E-R"]/text()').getall()
        content = "\b".join(response.xpath("//p/text()").getall()[4:])
        date = response.xpath("//time/text()").get()
        data = {
            "Date": date,
            "Headline": headline,
            "Tags": "|".join(tags),
            "Content": content,
        }

        self.__docs(**data).save()

        # Update companies' profile
        self.tag_cleaner.update_tags(tags, date)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(NewsSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(
            spider.spider_closed, signal=scrapy.signals.spider_closed
        )
        return spider

    def spider_closed(self, spider):
        self.newsdb.close()
