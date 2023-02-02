import importlib

# Fake Header
from faker import Faker

# Database connection
from Financial_data_crawler.db.clients import get_mongodb_news_conn
from Financial_data_crawler.db.router import update_data

# Replace requests method with httpx
from Financial_data_crawler.DataReader import HTTPClient

class TWDay_Trade_Info:

    def __init__(self):
        self.conn= get_mongodb_news_conn()['Comp']

    #@check_date(get_mongodb_news_conn()['Comp'])
    def crawler_select(self,crawler_name:str):
        markettype=crawler_name.split('_')[0]
        cleaner=f'TW{markettype}_opendata_cleaner'
        api = config.get('api',crawler_name)

        client = HttpClient()
        info = client.get_raw_data(api)

        cls= getattr(
            importlib.import_module(f"Financial_data_crawler.Data_Cleaner.twse_cleaner"),
            f"{cleaner}")()

        dataset=getattr(cls,crawler_name)(info)

        return dataset

        #update_data(crawler_name,dataset)


