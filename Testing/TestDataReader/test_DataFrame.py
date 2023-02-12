from Financial_data_crawler.DataReader.DataFrame import DataFrame
import pandas as pd
import pytest


class TestGetRawData:
    valid_api_sets = [
        r"https://quality.data.gov.tw/dq_download_csv.php?nid=11549&md5_url=da96048521360db9f23a2b47c9c31155"
    ]
    invalid_api_sets = [
        "123",
        r"https://quality.data.gov.tw/dq_download_xml.php?nid=11549&md5_url=da96048521360db9f23a2b47c9c31155",
        r"https://quality.data.gov.tw/dq_download_json.php?nid=11549&md5_url=da96048521360db9f23a2b47c9c31155",
    ]

    @pytest.mark.parametrize("api", valid_api_sets)
    def test_valid_csv_api(self, api):
        data = DataFrame().get_raw_data(api)
        assert isinstance(data, pd.DataFrame)

    @pytest.mark.parametrize("api", invalid_api_sets)
    def test_invalid_csv_api(self, api):
        data = DataFrame().get_raw_data(api)
        assert isinstance(data, pd.DataFrame)
