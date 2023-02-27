import re
import random
import time
from datetime import datetime, timedelta

import scrapy
from faker import Faker
from bs4 import BeautifulSoup
from loguru import logger

from Financial_data_crawler.db.clients import MongoClient
from Financial_data_crawler.db.ChipModels import Broker_Info, Broker_Transaction


client = MongoClient("Scrapy", "sel_broker")
fake = Faker()


def _extract_code_and_name(bs4_obj: BeautifulSoup)->str:
    pattern = r"GenLink2stk\('(\w+)','([^']+)'\);"
    name_obj = bs4_obj.find("script")

    if name_obj is not None:
        name_str = name_obj.text
        match = re.search(pattern, name_str)
    else:
        pattern = "^[A-Za-z0-9]+"
        name_str = bs4_obj.find("a").text
        match = re.search(pattern, name_str)

    if match and pattern == r"GenLink2stk\('(\w+)','([^']+)'\);":
        stockid, stockname = match.group(1), match.group(2)

    elif pattern == "^[A-Za-z0-9]+":
        stockid = match.group(0)
        stockname = name_str[match.span()[1] :]

    else:
        stockid = stockname = None

    return stockid, stockname

def _get_api_nontrading_day(default_date:str):
    """
    When implementing historical data fetching,
    programming will fetch the earliest day .
    If the day is a holiday or nontrading day,
    then automatically move backward to the day before it.
    """

    import requests

    def nontrading_day_test(date_str:str)->str:
        """
        Check whether the market opened that day
        """
        day_plus_one_day = _day_move_one_day(date_str, plus=True)
        url = f'https://just2.entrust.com.tw/z/zg/zgb/zgb0.djhtm?a=5920&b=5920&c=E&e={date_str}&f={date_str}'
        print(url)
        response = requests.get(url, headers={'User-Agent': fake.user_agent()})
        soup = BeautifulSoup(response.text, 'html.parser')
        compare_date_str = soup.find("div", {"class": "t11"}).text[-8:]
        print(compare_date_str)

        try:
            report_date = datetime.strptime(compare_date_str, '%Y%m%d')
            report_date_str = report_date.strftime('%Y-%#m-%#d')
            return report_date_str == date_str

        except:
            return False

    while not nontrading_day_test(default_date):
        default_date = _day_move_one_day(default_date)

    return default_date

def _day_move_one_day(date_str:str, plus:bool = False)->str:

    # Convert the string to a datetime object
    date = datetime.strptime(date_str, '%Y-%m-%d')

    # Subtract one day
    one_day = timedelta(days=1)

    if plus:
        new_date = date + one_day
    else:
        new_date = date - one_day

    # Convert back to a string in the same format
    return new_date.strftime('%Y-%#m-%#d')

def _reformat_date_str(date_str:str):
    date = datetime.strptime(date_str, '%Y-%m-%d')
    return date.strftime('%Y-%m-%d')

class SelBrokerSpider(scrapy.Spider):
    name = "sel_broker"
    allowed_domains = [
        "moneydj.emega.com.tw/",
        "stock.capital.com.tw/",
        "newjust.masterlink.com.tw/",
        "just2.entrust.com.tw/",
        "sjmain.esunsec.com.tw/",
        '5850web.moneydj.com/',
        'fubon-ebrokerdj.fbs.com.tw/'
    ]

    custom_settings = {
        'RETRY_DELAY': 60,
        'RETRY_TIMES': 3, # Set the maximum number of times to retry failed requests to 3
    }

    def __init__(self, date: str = None, auto_date:bool = False):
        self.date = date

        if auto_date:
            # Get the earliest date from the database
            earliest_transaction = Broker_Transaction.objects().order_by('Date').first()

            if earliest_transaction is not None:
                # Convert the date to a reasonable format
                earliest_date_str = _day_move_one_day(earliest_transaction['Date'])

                # Check if the date is a trading day
                reasonable_date = _get_api_nontrading_day(earliest_date_str)
                self.date = reasonable_date
            else:
                # Handle the case where there are no transactions in the database
                self.date = None

    def start_requests(self):
        def produce_broker_branch_urls(date: str = None):
            brokers = Broker_Info.objects()
            base_url = r"z/zg/zgb/zgb0.djhtm?"
            if date:
                # one_day_after = _day_move_one_day(date)
                date_str = f"&e={date}&f={date}"  # Only parse daily info
            else:
                date_str = ""

            start_urls = []

            for broker in brokers:
                broker_code = broker["BrokerCode"]
                broker_name = broker["BrokerName"]

                if broker["BrokerBranch"] is None:
                    broker_d = {broker_code: broker_name}

                else:
                    broker_d = broker["BrokerBranch"]

                for branch in broker_d.keys():

                    '''
                    # Primary key: broker_code,branch_code,date
                    # Don't add duplicated records
                    if Broker_Transaction.objects(
                        Date=_reformat_date_str(self.date),
                        BrokerCode=broker_code,
                        BranchCode=branch,
                        ).count()>0:
                        continue;
                    '''
                    if re.search("[A-Za-z]", branch):
                        branch_code = "".join(
                            [format(ord(c), "02x").zfill(4) for c in branch]
                        )
                    else:
                        branch_code = branch

                    brach_name = broker["BrokerBranch"][branch]
                    url_vol = (
                        base_url + f"a={broker_code}&b={branch_code}&c=E" + date_str
                    )
                    url_amt = (
                        base_url + f"a={broker_code}&b={branch_code}&c=B" + date_str
                    )
                    meta = {
                        "BrokerCode": broker_code,
                        "BrokerName": broker_name,
                        "BranchName": brach_name,
                        "BranchCode": branch,
                    }

                    start_urls.append({"url": url_vol, "meta": meta})
                    start_urls.append({"url": url_amt, "meta": meta})

            return start_urls

        start_urls = produce_broker_branch_urls(self.date)

        domains_num = len(self.allowed_domains)
        logger.info(f'request numbers-----{len(start_urls)}')
        for num,url in enumerate(start_urls):
            time.sleep(round(random.uniform(1, 4), 2))
            domain = self.allowed_domains[divmod(num,domains_num)[1]]
            if  domain != '5850web.moneydj.com/':
                http_header = "https://"
            else:
                http_header = "http://"
            full_url = http_header + domain + url["url"]

            headers = {
                "User-Agent": fake.user_agent()
            }

            logger.info(full_url)
            logger.info(headers)
            yield scrapy.Request(
                url=full_url,
                callback=self.parse,
                meta=url["meta"],
                headers = headers
            )

    def parse(self, response):
        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all("table")

        date_str = soup.find("div", {"class": "t11"}).text[-8:]
        date = datetime.strptime(date_str, "%Y%m%d")
        report_date = date.strftime("%Y-%m-%d")

        for i in range(3, 5):
            table = tables[i]

            header_line = table.find_all("tr")[1]
            header = [h.text for h in header_line.find_all("td")]

            if "買進金額" in header:
                name_mapping = {"買進金額": "BuyAmount", "賣出金額": "SellAmount"}
            else:
                name_mapping = {"買進張數": "BuyVolume", "賣出張數": "SellVolume"}

            for j, record in enumerate(table.find_all("tr")[2:]):
                data = {}
                data["BrokerCode"] = response.meta.get("BrokerCode")
                data["BrokerName"] = response.meta.get("BrokerName")
                data["BranchCode"] = response.meta.get("BranchCode")
                data["BranchName"] = response.meta.get("BranchName")
                data["Date"] = report_date

                # <a href="javascript:Link2Stk('00878');">00878國泰永續高股息</a> specia case!
                stockid, stockname = _extract_code_and_name(record)
                data["StockID"] = stockid
                data["StockName"] = stockname

                record_data = [
                    d.text.replace(r",", "") for d in record.find_all("td")[1:]
                ]
                data[name_mapping[header[1]]] = record_data[0]
                data[name_mapping[header[2]]] = record_data[1]

                Broker_Transaction.objects(
                    StockID=data["StockID"],
                    Date=data["Date"],
                    BrokerCode=data["BrokerCode"],
                    BranchCode=data["BranchCode"],
                ).modify(upsert=True, **data)

