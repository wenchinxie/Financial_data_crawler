import pandas as pd
from typing import List

def df_cleaner(df:pd.DataFrame,col_mapping:dict):
    '''
    Change column name and remove the prices containing null and weird words in order to store in database
    '''
    df = df.rename(columns=col_mapping)
    df = df[col_mapping.values()]
    if df['Max'].dtype != float:
        df = df[~df['Max'].str.contains('-| ')]

    return df

class TWListed_opendata_cleaner:
    def __init__(self):
        self.twlisted_stock_col={
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

    def Listed_Day_Transaction_Info(self,df:pd.DataFrame)->List[dict]:

        df = df_cleaner(df, self.twlisted_stock_col)

        return df.to_dict(orient='records')


class TWOTC_opendata_cleaner:
    def __init__(self):
        self.twotc_stock_col={
            '代號':"StockID",
            '名稱':'Name',
            '收盤':'Close',
            '漲跌':'Change',
            '開盤':'Open',
            '最高':'Max',
            '最低':'Min',
            '成交股數':'TradeVolume',
            '成交筆數': 'Transaction',
            "成交金額": "TradeValue"
        }

    def OTC_Day_Transaction_Info(self,df:pd.DataFrame)->List[dict]:
        df = df_cleaner(df, self.twotc_stock_col)
        return df.to_dict(orient='records')
