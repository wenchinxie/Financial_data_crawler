import importlib
# api linkage
from Financial_data_crawler import config_setup

config = config_setup.config


def crawler_select(crawler_name: str):
    cleaner = config.get('Cleaner', crawler_name)
    api = config.get('TWSE_api', crawler_name)
    reader_type = config.get('DataReader', crawler_name)

    client = getattr(importlib.import_module(f"Financial_data_crawler.DataReader.{reader_type}"),
                     f"{reader_type}")()
    data = client.get_raw_data(api)

    cls = getattr(
        importlib.import_module(f"Financial_data_crawler.DataCleaner.twse_cleaner"),
        f"{cleaner}")()

    dataset = getattr(cls, crawler_name)(data)

    return dataset
