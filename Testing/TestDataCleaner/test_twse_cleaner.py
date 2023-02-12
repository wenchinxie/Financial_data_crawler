from Financial_data_crawler.DataCleaner import twse_cleaner
import pandas as pd
import pytest
import yaml
from pathlib import Path
import os
from bs4 import BeautifulSoup


class TestDFCleaner:
    valid_no_change_data = {
        "StockID": ["2330", "1234"],
        "Max": [123, 1414],
    }
    valid_data_one = {
        "StockID": ["2245"],
        "Max": ["--"],
    }
    valid_data_two = {
        "StockID": ["2245"],
        "Max": [" "],
    }

    expected_data_one = {
        "StockID": ["2245"],
        "Max": [0.0],
    }
    valid_no_change_df = pd.DataFrame(data=valid_no_change_data)
    expected_no_change_df = pd.DataFrame(data=valid_no_change_data)

    valid_df_one = pd.DataFrame(data=valid_data_one)
    valid_df_two = pd.DataFrame(data=valid_data_two)
    expected_df_one = pd.DataFrame(data=expected_data_one)

    valid_df_set = [
        (valid_no_change_df, expected_no_change_df),
        (valid_df_one, expected_df_one),
        (valid_df_two, expected_df_one),
    ]

    @pytest.mark.parametrize("valid_df, expected_df", valid_df_set)
    def test_valid_df(self, valid_df, expected_df):
        clean_df = twse_cleaner.df_cleaner(valid_df)
        assert clean_df.equals(expected_df)

    @pytest.mark.parametrize("df", [123, "13232", pd.Series()])
    def test_invalid_df(self, df):
        with pytest.raises(TypeError) as exc_info:
            twse_cleaner.df_cleaner(df)


def test_valid_turntoint():
    res = twse_cleaner.turntoint("123,412")
    assert res == "123412"


def test_invalid_turntoint():
    with pytest.raises(TypeError) as exc_info:
        res = twse_cleaner.turntoint(123)


def yaml_data_with_file():
    with open(
        os.path.join(Path(__file__).parent, "cleaner_test_data.yaml"), encoding="utf-8"
    ) as f:
        return yaml.safe_load(f)


def yaml_data_with_key(key):
    return yaml_data_with_file()[key]


class TestTWOtherCleaner:
    @pytest.mark.parametrize("case, expected", yaml_data_with_key("DayTradeTestData"))
    def test_parse_table(self, case, expected):
        data_to_soup_obeject = BeautifulSoup(case, "html.parser")
        cleaner = twse_cleaner.TWOther_cleaner()
        cleaner.parse_day_trade_table(data_to_soup_obeject)
        assert cleaner.res == [expected]
