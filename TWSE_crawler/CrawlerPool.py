import importlib
# api linkage
from Financial_data_crawler import config_setup

config=config_setup.config

def crawler_select(crawler_name: str):
    markettype = crawler_name.split('_')[0]
    cleaner = f'TW{markettype}_opendata_cleaner'
    api = config.get('TWSE_api', crawler_name)

    reader_type = config.get('DataReader',crawler_name)

    client = getattr(importlib.import_module(f"Financial_data_crawler.DataReader.{reader_type}"),
        f"{reader_type}")()
    data = client.get_raw_data(api)

    cls = getattr(
        importlib.import_module(f"Financial_data_crawler.Data_Cleaner.twse_cleaner"),
        f"{cleaner}")()

    dataset = getattr(cls, crawler_name)(data)

    return dataset