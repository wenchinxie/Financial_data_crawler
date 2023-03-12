import re
import time
import random
import os
import asyncio
import aiofiles
import shutil
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import tracemalloc

import chardet
from bs4 import BeautifulSoup
from loguru import logger
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from mongoengine import *
from pymongo import UpdateOne,errors
from faker import Faker

from Financial_data_crawler.db.clients import MongoClient,get_motor_conn
from Financial_data_crawler.db.ChipModels import Broker_Info, Broker_Transaction


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
        response = requests.get(url, headers={'User-Agent': Faker().user_agent()})
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

class SelBroker:
    client = MongoClient("Scrapy", "sel_broker")

    def __init__(self,date:str=None, auto_date:bool=False):
        self.date = date

        if auto_date:
            # Get the earliest date from the database
            earliest_transaction = Broker_Transaction.objects().order_by('Date').first()

            if earliest_transaction is not None:
                # Convert the date to a reasonable format
                earliest_date_str = _day_move_one_day(earliest_transaction['Date'].strftime('%Y-%m-%d'))

                # Check if the date is a trading day
                reasonable_date = _get_api_nontrading_day(earliest_date_str)
                self.date = reasonable_date
            else:
                # Handle the case where there are no transactions in the database
                self.date = None

        self.urls = self.produce_broker_branch_urls(self.date)

    def produce_broker_branch_urls(self, date: str = None):
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

                start_urls.append({"url": url_vol, "meta": meta, "type":'E'})
                start_urls.append({"url": url_amt, "meta": meta, "type":"B"})
                SelBroker.client.close()

        return start_urls

    def parse(self):

        allow_domains = ['https://just2.entrust.com.tw/',
                         'https://fubon-ebrokerdj.fbs.com.tw/',
                         'http://5850web.moneydj.com/',
                         'https://stock.capital.com.tw/',
                         'https://newjust.masterlink.com.tw/',
                         'https://jsjustweb.jihsun.com.tw/',
                         'https://sjmain.esunsec.com.tw/']

        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument('--no-sandbox')
        options.add_argument("--pageLoadStrategy=normal")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        chromedriver_path = '/home/wenchin/airflow/dags/chromedriver'
        driver = webdriver.Chrome(executable_path=chromedriver_path, options=options)
        today = datetime.today()
        date_string = today.strftime('%Y%m%d%H%M%S')

        for num, link in enumerate(self.urls):
            time.sleep(random.randint(8, 13))
            try:

                cur_url =f"{allow_domains[divmod(num,len(allow_domains))[1]]}{link['url']}"
                logger.info(cur_url)
                driver.implicitly_wait(300)
                driver.get(cur_url)
            except:

                cur_url = f"{allow_domains[divmod(num,len(allow_domains))[1]-3]}{link['url']}"
                logger.info(cur_url)
                driver.implicitly_wait(300)
                driver.get(cur_url)

            logger.debug(cur_url)

            try:
                wait = WebDriverWait(driver, 600)
                wait.until(EC.presence_of_element_located(
                    (By.XPATH, '//*[@id="oMainTable"]/tbody/tr[3]/td[1]/table/tbody/tr[1]/td')))
            except TimeoutError as e:
                time.sleep(1800)
                driver.get(cur_url)
                wait = WebDriverWait(driver, 600)
                wait.until(EC.presence_of_element_located(
                    (By.XPATH, '//*[@id="oMainTable"]/tbody/tr[3]/td[1]/table/tbody/tr[1]/td')))

            filename = f''
            for key, value in link['meta'].items():
                filename = filename + key + "-" + value + '_'

            with open(f"/home/wenchin/airflow/dags/data/sel_broker_data/{filename}_{link['type']}_{date_string}.txt", 'w') as f:
                f.write(driver.page_source)

        driver.quit()

class SelBrokerDataCrawler:

    CONN = get_motor_conn()
    DB=CONN['Chip']['broker__transaction']

    @staticmethod
    async def run():
        tracemalloc.start()
        loop = asyncio.get_event_loop()
        result = []
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            tasks = []
            for root, dirs, files in os.walk(r"/home/wenchin/airflow/dags/data/sel_broker_data", topdown=False):
                for file in files[:1000]:
                    tasks.append(await loop.run_in_executor(executor, SelBrokerDataCrawler.parse, root, file))
            result.extend(await asyncio.gather(*tasks))

    @staticmethod
    async def parse(root, filename):
        """
        Parses financial data from an HTML file and saves it to a database.
        """

        response = None
        async with aiofiles.open(os.path.join(root, filename), 'rb') as f:
            content = await f.read()
            encoding = chardet.detect(content)['encoding']

        try:
            async with aiofiles.open(os.path.join(root, filename), 'r' ,encoding=encoding) as f:
                response = await f.read()
        except:
            try:
                async with aiofiles.open(os.path.join(root, filename), 'r') as f:
                    response = await f.read()
                print('natural selection failed')
            except Exception as e:
                logger.debug(e)
                async with aiofiles.open(os.path.join(root, filename), 'r',encoding='big5',errors='ignore') as f:
                    response = await f.read()

        finally:
            if not response:
                return await asyncio.to_thread(shutil.move,
                    os.path.join(root, filename),
                    os.path.join(r'/home/wenchin/airflow/dags/data/UnicodeError', filename)
                )

        print('Getting response...')

        meta_data = {}
        for name in filename.split('_'):
            # print(name)
            if '-' in name:
                key, value = name.split('-')[0], '-'.join(name.split('-')[1:])
                meta_data[key] = value

        soup = BeautifulSoup(response, "html.parser")

        trans_data_str = soup.find("td", {'id': 't3n0'})
        if trans_data_str:
            if soup.find("td", {'id': 't3n0'}).text =='無此券商分點交易資料':
                return await asyncio.to_thread(shutil.move,
                                               os.path.join(root, filename),
                                               os.path.join(r'/home/wenchin/airflow/dags/data/completed_data', filename)
                                               )

        tables = soup.find("table",{'id':'oMainTable'}).find_all('table')

        date_str = soup.find("div", {"class": "t11"}).text[-8:]
        try:
            meta_data['Date']=datetime.strptime(date_str, "%Y%m%d")
        except ValueError as e:
            logger.debug(e)
            return await asyncio.to_thread(shutil.move,
                os.path.join(root, filename),
                os.path.join(r"/home/wenchin/airflow/dags/data/completed_data", filename)
            )


        data_list = []
        for table in tables[:3]:
            header_line = table.find_all("tr")[1]
            header = [h.text for h in header_line.find_all("td")]

            if "買進金額" in header:

                name_mapping = {"買進金額": "BuyAmount", "賣出金額": "SellAmount"}
                type = "Amount"
            else:
                name_mapping = {"買進張數": "BuyVolume", "賣出張數": "SellVolume"}
                type = 'Volume'

            meta_data['TransactionType'] = type

            html_tags = table.find_all("tr")[2:]

            for record in html_tags:
                data = meta_data.copy()

                stockid, stockname = SelBrokerDataCrawler._extract_stockinfo(record)
                data["StockID"] = stockid
                data["StockName"] = stockname

                record_data = [d.text.replace(r",", "") for d in record.find_all("td")[1:]]
                try:
                    data[name_mapping[header[1]]] = record_data[0]
                    data[name_mapping[header[2]]] = record_data[1]
                    data_list.append(data)
                except Exception as e:
                    logger.debug(e)
                    return await asyncio.to_thread(shutil.move,
                        os.path.join(root, filename),
                        os.path.join(r'/home/wenchin/airflow/dags/data/problem', filename)
                    )
        try:
            await SelBrokerDataCrawler.DB.insert_many(data_list, ordered=False)
            await asyncio.to_thread(shutil.move,
                os.path.join(root, filename),
                os.path.join(r'/home/wenchin/airflow/dags/data/completed_data', filename)
            )
        except errors.BulkWriteError as e:
            logger.debug(e)
            for error in e.details['writeErrors']:
                if not error['code'] == 11000:  # 11000 is the error code for duplicate key error
                    return await asyncio.to_thread(shutil.move,
                        os.path.join(root, filename),
                        os.path.join(r'/home/wenchin/airflow/dags/data/problem', filename)
                    )

            await asyncio.to_thread(shutil.move,
                os.path.join(root, filename),
                os.path.join(r'/home/wenchin/airflow/dags/data/completed_data', filename)
            )


    async def _combine_stock_info(data:list):
        combined_data = {}

        for item in data:
            key = (item['BrokerCode'], item['BranchCode'], item['Date'], item['StockID'])

            if key in combined_data:
                # Update existing key-value pairs with new information
                combined_data[key]['BuyAmount'] = item.get('BuyAmount', 0) + combined_data[key].get('BuyAmount', 0)
                combined_data[key]['SellAmount'] = item.get('SellAmount', 0) + combined_data[key].get('SellAmount', 0)
                combined_data[key]['BuyVolume'] = item.get('BuyVolume', 0) + combined_data[key].get('BuyVolume', 0)
                combined_data[key]['SellVolume'] = item.get('SellVolume', 0) + combined_data[key].get('SellVolume', 0)
            else:
                # Create new key-value pair with all available information
                combined_data[key] = {
                    'BrokerCode': item['BrokerCode'],
                    'BrokerName': item['BrokerName'],
                    'BranchCode': item['BranchCode'],
                    'BranchName': item['BranchName'],
                    'Date': item['Date'],
                    'StockID': item['StockID'],
                    'StockName': item['StockName'],
                    'BuyAmount': item.get('BuyAmount', 0),
                    'SellAmount': item.get('SellAmount', 0),
                    'BuyVolume': item.get('BuyVolume', 0),
                    'SellVolume': item.get('SellVolume', 0)
                }

        return combined_data

    @staticmethod
    def _extract_stockinfo(bs4_obj: BeautifulSoup) -> str:


        try:
            name_obj = bs4_obj.find("script")

            if name_obj is not None:
                pattern = r"GenLink2stk\('(\w+)','([^']+)'\);"
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
                stockname = name_str[match.span()[1]:]

            else:
                stockid = stockname = None
        except AttributeError:
            asyncio.to_thread(shutil.move,
                                    os.path.join(root, filename),
                                    os.path.join(r'/home/wenchin/airflow/dags/data/problem', filename)
                                    )

        return stockid, stockname




