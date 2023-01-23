import importlib

from Financial_data_crawler.db import router,clients
from Financial_data_crawler import config_setup

from Financial_data_crawler.DataReader import HttpClient
config = config_setup.config

def main(crawler_type:str,crawler_name:str):
    client = clients.MongoClient(crawler_type, crawler_name)

    r = router.Router(crawler_type,crawler_name)
    recent_date=r.recent_date()
    date_checker= DateChecker(crawler_name)
    update_date=date_checker.update
    print(recent_date,update_date)

    assert update_date is not None ,'Please Check the update day, should not be None'

    if not recent_date =='Update' or recent_date == update_date:
        print('Already Up to date')
        client.close()
        return None

    func = importlib.import_module(f'Financial_data_crawler.{crawler_type}_crawler.CrawlerPool')
    dataset = getattr(func,'crawler_select')(crawler_name)
    r.update_data(dataset,update_date)
    client.close()


class DateChecker:
    def __init__(self,crawler_name:str):
        self.__check_type = config.get('DateCheck',crawler_name)
        self.update = getattr(self, self.__check_type)()

    def MarketDay(self):
        client = HttpClient.HttpClient()
        return client.get_market_day()