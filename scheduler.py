import importlib
import datetime
from Financial_data_crawler.db import router, clients
from Financial_data_crawler import config_setup

from Financial_data_crawler.DataReader import HTTPClient

config = config_setup.config


def main(crawler_type: str, crawler_name: str):
    # Connect to Database
    client = clients.MongoClient(crawler_type, crawler_name)

    # Select the router
    r = router.Router(crawler_type, crawler_name)

    # Need to update?
    update_date = date_compare(r, crawler_name)

    if update_date is None:
        client.close()
        return None

    # Parse Data
    func = importlib.import_module(
        f"Financial_data_crawler.{crawler_type}_crawler.CrawlerPool"
    )

    # Get Raw Data
    dataset ,args = getattr(func, "crawler_select")(crawler_name)
    print('Dataset!',dataset)
    # Update to database
    r.update_data(dataset, update_date ,*args)
    client.close()


def date_compare(r, crawler_name: str):
    """If the Data is newest then cancel the update"""
    recent_date = r.recent_date()
    date_checker = DateChecker(crawler_name)
    update_date = date_checker.update
    print(recent_date, update_date)

    assert update_date is not None, "Please Check the update day, should not be None"

    # if not recent_date == 'Update' or recent_date == update_date:
    if recent_date == update_date:
        print("Already Up to date")
        return None

    return update_date


class DateChecker:
    def __init__(self, crawler_name: str):
        self.__check_type = config.get("DateCheck", crawler_name)
        self.update = getattr(self, self.__check_type)()

    @staticmethod
    def marketday():
        client = HTTPClient.HTTPClient()
        return client.get_market_day()

    @staticmethod
    def season():
        """Return each quarter time for validation of date checker function"""
        """
        today = datetime.datetime.today()
        cur_year = today.year
        candidates = [
            datetime.datetime(cur_year - 1, 11, 15),
            datetime.datetime(cur_year, 3, 31),
            datetime.datetime(cur_year, 5, 15),
            datetime.datetime(cur_year, 8, 15),
            datetime.datetime(cur_year, 11, 15),
        ]

        closest = max(candidates, key=lambda d: abs(today - d))
        return closest.strftime("%Y-%m-%d")
        """

        return datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
