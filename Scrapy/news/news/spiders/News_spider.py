import scrapy
import pandas as pd
import sqlite3
import json
import requests
import datetime

def get_urls():
    urls=[]
    start = datetime.datetime(2022,8,14).timestamp()
    while (start > datetime.datetime(2022,1,1).timestamp()):

        old = start - 2592000

        print('Time Period:',datetime.date.fromtimestamp(old).strftime("%Y-%m-%d"),'-',datetime.date.fromtimestamp(start).strftime("%Y-%m-%d"))
        linkage = f'https://news.cnyes.com/api/v3/news/category/tw_stock?startAt={int(round(old,0))}&endAt={int(round(start,0))}&limit=30&page={1}'
        raw_text = json.loads(requests.get(linkage).text)
        last_page = raw_text['items']['last_page']

        for i in range(1,last_page+1):
            linkage = f'https://news.cnyes.com/api/v3/news/category/tw_stock?startAt={int(round(old,0))}&endAt={int(round(start,0))}&limit=30&page={i}'
            raw_text = json.loads(requests.get(linkage).text)

            for newsid in raw_text['items']['data']:
                url=f"https://news.cnyes.com/news/id/{newsid['newsId']}?exp=a"
                if not url in urls:urls.append(url)

        start-=2592000

    return urls

class NewsSpider(scrapy.Spider):
    name = "News"
    start_urls = get_urls()
    if len(start_urls)==0: raise ValueError('ha')\

    allowed_domains =['https://news.cnyes.com/']

    def parse(self, response):

        conn = sqlite3.connect(r'C:\Users\s3309\Website\AT_WEB\db\db.sqlite3')

        data=pd.DataFrame()
        headline= response.xpath('//h1[@itemprop="headline"]/text()').get()
        tags='|'.join(response.xpath('//span[@class="_1E-R"]/text()').getall())
        content='\b'.join(response.xpath('//p/text()').getall()[4:])
        time=response.xpath('//time/text()').get()

        c=conn.cursor()
        c.execute(f"INSERT INTO News ('Date', 'Headline', 'Tags', 'Content') VALUES (?,?,?,?)",[time,headline,tags,content])

        conn.commit()
        conn.close()

