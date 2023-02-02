import pytest
from Financial_data_crawler.DataReader.HTTPClient import HTTPClient
import re

@pytest.fixture
def cur_market_day():
    return HTTPClient().get_market_day()

class TestGetRawData:

    valid_api_sets = [
        r'https://ithelp.ithome.com.tw/articles/10288291',
        r'https://tw.stock.yahoo.com/quote/2303.TW'
    ]

    invalid_api_sets =[1,66.87]

    @pytest.mark.parametrize('urls', valid_api_sets)
    def test_valid_api_url(self, urls):
        response = HTTPClient().get_raw_data(urls)
        return isinstance(response.status_code, int)

    @pytest.mark.parametrize('urls', invalid_api_sets)
    def test_invalid_api_url(self, urls):
        with pytest.raises(ValueError) as exc_info:
            response = HTTPClient().get_raw_data(urls)

class TestMarketDay:
    def test_get_market_day(self, cur_market_day):
        res = re.match(r'\d{4}/\d{2}/\d{2}', cur_market_day)
        assert res is not None

