import importlib
from Financial_data_crawler import config_setup
config = config_setup.config


def crawler_select(crawler_name: str):
    # reader_type = config.get('DataReader', crawler_name)

    # client = getattr(importlib.import_module(f"Financial_data_crawler.DataReader.{reader_type}"),
    #                 f"{reader_type}")()
    # data = client.get_raw_data(api)
    modelname = ''.join(crawler_name.split('_'))
    cls = getattr(
        importlib.import_module(f"Financial_data_crawler.Data_Cleaner.financial_report_cleaner"),
        modelname)()

    dataset = getattr(cls, 'get_data')()

    return dataset
