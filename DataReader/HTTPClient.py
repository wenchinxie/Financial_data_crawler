import httpx
import chardet
from faker import Faker
from bs4 import BeautifulSoup
import datetime

def autodetect(content):
    return chardet.detect(content).get("encoding")


class HTTPClient:
    def __init__(self):
        self.client = httpx.Client(default_encoding=autodetect)
        self.header = Faker().user_agent()

    def get_raw_data(self, api_url: str):
        if not isinstance(api_url, str) or not api_url.startswith('http'):
            raise ValueError(f'Not Valid URL format: {api_url}')

        response = self.client.get(api_url, headers={"User-Agent": self.header})
        self.check_status_code(response)

        return response

    def get_market_day(self):
        response = self.client.get(r'https://tw.stock.yahoo.com/quote/2330.TW', headers={"User-Agent": self.header})
        self.check_status_code(response)

        soup = BeautifulSoup(response.text, 'html.parser')
        market_day = soup.find('span', {'class': "C(#6e7780) Fz(12px) Fw(b)"}).text
        market_day = market_day.split(' ')[2]

        if market_day == '-':
            return datetime.datetime.today().strftime("%Y-%m-%d")

        return market_day

    @staticmethod
    def check_status_code(response):

        if response.status_code != 200:
            return ValueError("Fail to get the target url")
