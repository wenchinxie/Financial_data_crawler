import httpx
import chardet
from faker import Faker
from bs4 import BeautifulSoup
import datetime


def autodetect(content):
    return chardet.detect(content).get("encoding")

class BaseClient:
    def __init__(self):
        self.client = httpx.Client(default_encoding=autodetect)
        self.header = Faker().user_agent()

    def _parse_html(self,url):
        response = self.client.get(url, headers={"User-Agent": self.header})
        self.check_status_code(response)
        return BeautifulSoup(response.text, "html.parser")

    @staticmethod
    def check_status_code(response):
        if response.status_code != 200:
            raise ValueError("Fail to get the target url")


class HTTPClient(BaseClient):
    def __init__(self):
        super().__init__()

    def get_raw_data(self, api_url: str):
        if not isinstance(api_url, str) or not api_url.startswith("http"):
            raise ValueError(f"Not Valid URL format: {api_url}")

        response = self.client.get(api_url, headers={"User-Agent": self.header})
        self.check_status_code(response)

        return response

    def get_market_day(self):
        soup = self._parse_html(r"https://tw.stock.yahoo.com/quote/2330.TW")
        market_day = soup.find("span", {"class": "C(#6e7780) Fz(12px) Fw(b)"}).text
        market_day = market_day.split(" ")[2]

        if market_day == "-":
            return datetime.datetime.today().strftime("%Y-%m-%d")

        return market_day

class IndParser(BaseClient):
    def __init__(self):
        super().__init__()
        self._url = "https://ic.tpex.org.tw/introduce.php"
        self._all_inds = None


    def get_inds_comps(self):
        self._get_all_inds()

        for


        return

    def _get_all_inds(self):
        response = client.get(self._url, headers={"User-Agent": header})
        soup = BeautifulSoup(response.text, 'html.parser')
        self._all_inds = {ind['value']: ind.text.strip() for ind in soup.find(id='ic_option').find_all('option')}
