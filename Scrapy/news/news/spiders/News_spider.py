import scrapy
import psycopg2
import json
import requests
import datetime

def get_urls(start_date,end_date):
    urls=[]
    start = datetime.datetime.strptime(start_date,"%Y-%m-%d").timestamp()
    end =datetime.datetime.strptime(end_date,"%Y-%m-%d").timestamp()
    while (start > end):

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
    allowed_domains =['https://news.cnyes.com/']

    def __init__(self,start_date,end_date):
        self.start_urls = get_urls(start_date,end_date)

    def parse(self, response):

        headline= response.xpath('//h1[@itemprop="headline"]/text()').get()
        tags='|'.join(response.xpath('//span[@class="_1E-R"]/text()').getall())
        content='\b'.join(response.xpath('//p/text()').getall()[4:])
        date=response.xpath('//time/text()').get()

        conn = psycopg2.connect(
            host="db",
            database="postgres",
            user="postgres",
            password="postgres")

        with conn:
            with conn.cursor() as c:
                insert_sql = "INSERT INTO News ('Date', 'Headline', 'Tags', 'Content') VALUES (%s,%s,%s,%s)\
                        ON CONFLICT ('Date', 'Headline') DO UPDATE \
                        SET (Tags,Content) = (EXCLUDED.Tags,EXCLUDED.Content)" 

                c.execute(insert_sql,(date,headline,tags,content))
                conn.commit()
                yield {'date':date,'headline':headline}

