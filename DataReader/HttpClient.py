import httpx
import chardet
from faker import Faker
from bs4 import BeautifulSoup


def autodetect(content):
    return chardet.detect(content).get("encoding")


class HttpClient:
    def __init__(self):
        self.client = httpx.Client(default_encoding=autodetect)
        self.header = Faker().user_agent()

    def get_raw_data(self, api: str):
        return self.client.get(api, headers={"User-Agent": self.header})

    def get_market_day(self):
        response = self.client.get(r'https://tw.stock.yahoo.com/quote/2330.TW', headers={"User-Agent": self.header})
        soup = BeautifulSoup(response.text, 'html.parser')
        market_day = soup.find('span', {'class': "C(#6e7780) Fz(12px) Fw(b)"}).text
        market_day = market_day.split(' ')[2]
        return market_day
