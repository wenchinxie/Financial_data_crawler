import re
import random
import time
from datetime import datetime

import scrapy
from faker import Faker
from bs4 import BeautifulSoup
from loguru import logger

from Financial_data_crawler.db.clients import MongoClient
from Financial_data_crawler.db.ChipModels import Broker_Info, Broker_Transaction

client = MongoClient("Scrapy", "sel_broker")
fake = Faker()

def _extract_code_and_name(bs4_obj:BeautifulSoup):

    pattern = r"GenLink2stk\('(\w+)','([^']+)'\);"
    name_obj = bs4_obj.find('script')

    if name_obj is not None:
        name_str = name_obj.text
        match = re.search(pattern, name_str)
    else:
        pattern = '^[A-Za-z0-9]+'
        name_str = bs4_obj.find('a').text
        match = re.search(pattern, name_str)

    if match and pattern == r"GenLink2stk\('(\w+)','([^']+)'\);":
        stockid, stockname = match.group(1), match.group(2)

    elif pattern == '^[A-Za-z0-9]+':
        stockid = match.group(0)
        stockname = name_str[match.span()[1]:]

    else:
        stockid = stockname = None

    return stockid,stockname



class SciSpider(scrapy.Spider):
    name = "sel_broker"
    allowed_domains = [
        "moneydj.emega.com.tw",
        "5850web.moneydj.com",
        "stock.capital.com.tw",
        "newjust.masterlink.com.tw",
        "just2.entrust.com.tw",
        "sjmain.esunsec.com.tw",
        "jsjustweb.jihsun.com.tw",
    ]

    custom_settings = {"RETRY_MAX_TIME": 0}

    def __init__(self, date:str = None):
        self.date = date

    def start_requests(self):

        def produce_broker_branch_urls(date:str = None):
            brokers = Broker_Info.objects()
            base_url = r'/z/zg/zgb/zgb0.djhtm?'
            if date:
                date_str = f'&e={date}&f={date}' # Only parse daily info
            else:
                date_str = ''

            start_urls = []

            for broker in brokers:
                broker_code = broker['BrokerCode']
                broker_name = broker['BrokerName']

                if broker['BrokerBranch'] is None:
                    broker_d = {broker_code: broker_name}

                else:
                    broker_d = broker['BrokerBranch']

                for branch in broker_d.keys():
                    if re.search('[A-Za-z]', branch):
                        branch_code = ''.join([format(ord(c), '02x').zfill(4) for c in branch])
                    else:
                        branch_code = branch

                    brach_name = broker['BrokerBranch'][branch]
                    url_vol =  base_url + f'a={broker_code}&b={branch_code}&c=E' + date_str
                    url_amt = base_url + f'a={broker_code}&b={branch_code}&c=B' + date_str
                    meta = {'BrokerCode': broker_code,
                            'BrokerName': broker_name,
                            'BranchName': brach_name,
                            'BranchCode': branch}

                    start_urls.append({'url':url_vol,'meta':meta})
                    start_urls.append({'url':url_amt,'meta':meta})

            return start_urls

        start_urls = produce_broker_branch_urls(self.date)
        logger.info(start_urls)
        for url in start_urls:

            time.sleep(round(random.uniform(1, 4), 2))
            domain = random.choice(self.allowed_domains)
            full_url = 'https://' + domain + url['url']
            logger.info(full_url)
            yield scrapy.Request(
                url=full_url,
                callback=self.parse,
                headers={"User-Agent": fake.user_agent()},
                meta=url['meta']
            )

    def parse(self, response):

        soup = BeautifulSoup(response.text,'html.parser')
        tables = soup.find_all('table')


        date_str = soup.find('div', {'class': 't11'}).text[-8:]
        date = datetime.strptime(date_str, '%Y%m%d')
        report_date = date.strftime('%Y-%m-%d')

        for i in range(3,5):
            table = tables[i]

            header_line = table.find_all('tr')[1]
            header = [h.text for h in header_line.find_all('td')]

            if '買進金額' in header:
                name_mapping = {
                    '買進金額': 'BuyAmount',
                    '賣出金額': 'SellAmount'
                }
            else:
                name_mapping = {
                    '買進張數': 'BuyVolume',
                    '賣出張數': 'SellVolume'
                }

            for j, record in enumerate(table.find_all('tr')[2:]):
                data = {}
                data['BrokerCode'] = response.meta.get('BrokerCode')
                data['BrokerName'] = response.meta.get('BrokerName')
                data['BranchCode'] = response.meta.get('BranchCode')
                data['BranchName'] = response.meta.get('BranchName')
                data['Date'] = report_date

                # <a href="javascript:Link2Stk('00878');">00878國泰永續高股息</a> specia case!
                stockid, stockname = _extract_code_and_name(record)
                data['StockID'] = stockid
                data['StockName'] = stockname

                record_data = [d.text.replace(r',','') for d in record.find_all('td')[1:]]
                data[name_mapping[header[1]]] = record_data[0]
                data[name_mapping[header[2]]] = record_data[1]

                Broker_Transaction.objects(StockID = data['StockID'],
                                           Date = data['Date'],
                                           BrokerCode = data['BrokerCode'],
                                           BranchCode = data['BranchCode']
                                           ).modify(upsert=True,**data)















