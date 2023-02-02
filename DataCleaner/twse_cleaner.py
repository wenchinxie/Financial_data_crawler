import pandas as pd
from typing import List
from bs4 import BeautifulSoup


def df_cleaner(df: pd.DataFrame, col_mapping: dict):
    """
    Change column name and remove the prices containing null and weird words in order to store in database
    """

    df = df.rename(columns=col_mapping)
    df = df[col_mapping.values()]
    if df['Max'].dtype != float:
        df = df[~df['Max'].str.contains('-| ')]

    return df


class TWListed_opendata_cleaner:
    def __init__(self):
        self.twlisted_stock_col = {
            "證券代號": "StockID",
            "證券名稱": "Name",
            "成交股數": "TradeVolume",
            "成交筆數": "Transaction",
            "成交金額": "TradeValue",
            "開盤價": "Open",
            "最高價": "Max",
            "最低價": "Min",
            "收盤價": "Close",
            "漲跌價差": "Change"
        }

    def Listed_Day_Transaction_Info(self, df: pd.DataFrame) -> List[dict]:
        df = df_cleaner(df, self.twlisted_stock_col)

        return df.to_dict(orient='records')


class TWOTC_opendata_cleaner:
    def __init__(self):
        self.twotc_stock_col = {
            '代號': "StockID",
            '名稱': 'Name',
            '收盤': 'Close',
            '漲跌': 'Change',
            '開盤': 'Open',
            '最高': 'Max',
            '最低': 'Min',
            '成交股數': 'TradeVolume',
            '成交筆數': 'Transaction',
            "成交金額": "TradeValue"
        }

    def OTC_Day_Transaction_Info(self, df: pd.DataFrame) -> List[dict]:
        df = df_cleaner(df, self.twotc_stock_col)
        return df.to_dict(orient='records')


def turntoint(s: str, i: int, minimium: int):
    if i < minimium:
        return s

    new = s.replace(',', '')
    if not (new[0].isnumeric() and new[-1].isnumeric()):
        new = '0'

    return new


class TWOther_cleaner:

    @staticmethod
    def All_Day_Trade(response) -> List[dict]:
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find_all('table')[1]

        header_mapping = {
            '證券代號': 'StockID',
            '證券名稱': 'Name',
            '暫停現股賣出後現款買進當沖註記': 'BuyAfterSale',
            '當日沖銷交易成交股數': 'Volume',
            '當日沖銷交易買進成交金額': 'BuyAmount',
            '當日沖銷交易賣出成交金額': 'SellAmount'
        }

        headers = [header_mapping[header.text] for header in table.find_all('tr')[1].find_all('td')]
        res = []
        for i, n in enumerate(table.find_all('tr')[2:]):
            d = {header: turntoint(info.text, j, 3) for header, (j, info) in zip(headers, enumerate(n.find_all('td')))}
            d['Type'] = 'All'
            res.append(d)

        return res
