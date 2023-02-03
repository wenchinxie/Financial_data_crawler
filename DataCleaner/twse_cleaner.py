import pandas as pd
from typing import List
from bs4 import BeautifulSoup


def df_cleaner(df: pd.DataFrame) -> pd.DataFrame:
    """
    Change column name and remove the prices containing null and weird words in order to store in database
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError('Please In Pandas DataFrame Format')

    need_change_col = ['Max', 'Change']
    for col in need_change_col:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col].fillna(0, inplace=True)

    return df


def rename_col(df: pd.DataFrame, col_mapping: dict) -> pd.DataFrame:
    rename_col_df = df.rename(columns=col_mapping)
    rename_col_df = rename_col_df[col_mapping.values()]
    return rename_col_df


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
        rename_col_df = rename_col(df, self.twlisted_stock_col)
        clean_df = df_cleaner(rename_col_df)

        return clean_df.to_dict(orient='records')


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
        rename_col_df = rename_col(df, self.twotc_stock_col)
        clean_df = df_cleaner(rename_col_df)
        return clean_df.to_dict(orient='records')


def turntoint(s: str) -> str:
    if not isinstance(s, str):
        raise TypeError(f'{s} must be a string')

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
            '當日沖銷交易成交股數': 'DayTradeVolume',
            '當日沖銷交易買進成交金額': 'DayTradeBuyAmount',
            '當日沖銷交易賣出成交金額': 'DayTradeSellAmount'
        }


        headers = [header_mapping[header.text] for header in table.find_all('tr')[1].find_all('td')]
        res = []
        for i, n in enumerate(table.find_all('tr')[2:]):
            required_data = {}
            for header, data in zip(headers, n.find_all('td')):
                new_data = data
                if header in ['DayTradeVolume', 'DayTradeBuyAmount', 'DayTradeSellAmount']:
                    new_data = turntoint(data)

                required_data[header] = new_data

            res.append(required_data)

        return res
