import pandas as pd
from typing import List
from bs4 import BeautifulSoup


def df_cleaner(df: pd.DataFrame) -> pd.DataFrame:
    """
    Change column name and remove the prices containing null and weird words in order to store in database
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Please In Pandas DataFrame Format")

    need_change_col = ["Max", "Change"]
    for col in need_change_col:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col].fillna(0, inplace=True)

    return df


def rename_col(df: pd.DataFrame, col_mapping: dict) -> pd.DataFrame:
    rename_col_df = df.rename(columns=col_mapping)
    rename_col_df = rename_col_df[col_mapping.values()]
    return rename_col_df

def turn_year(row):
    date_str = str(row)
    return str(int(date_str[:3])+1911)+date_str[3:]

def get_company_news_col():
    return {
        '出表日期':'published_date',
        '發言日期':'announced_date',
        '事實發生日':'happened_date',
        '公司代號':'stock_id',
        '公司名稱':'stock_name',
        '主旨':'title',
        '符合條款':'clause',
        '說明':'description',
    }


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
            "漲跌價差": "Change",
        }

    def Listed_Day_Transaction_Info(self, df: pd.DataFrame) -> List[dict]:
        rename_col_df = rename_col(df, self.twlisted_stock_col)
        clean_df = df_cleaner(rename_col_df)

        return clean_df.to_dict(orient="records") , []

    def Listed_Spread_Shareholdings(self, df: pd.DataFrame) -> List[dict]:
        col_mapping ={
            '資料日期': 'Date',
            '證券代號': 'StockID',
            '持股分級': 'Level',
            '人數': 'People',
            '股數': 'Shares',
            '占集保庫存數比例%': 'Percent'
        }

        levels_mapping = {
            1 : '1-999',
            2 : '1000-5000',
            3 : '5001-10000',
            4 : '10001-15000',
            5 : '15001-20000',
            6 : '20001-30000',
            7 : '30001-40000',
            8 : '40001-50000',
            9 : '50001-100000',
            10 : '100001-200000',
            11 : '200001-400000',
            12 : '400001-600000',
            13 : '600001-800000',
            14 : '800001-1000000',
            15 : '1000001',
            17 : 'Sum'
        }

        rename_col_df = rename_col(df , col_mapping)
        remove_redundancy_df = rename_col_df[rename_col_df['Level']!=16]
        remove_redundancy_df['Level'] = remove_redundancy_df['Level'].map(levels_mapping)
        remove_redundancy_df['Date'] =pd.to_datetime(remove_redundancy_df['Date'],format='%Y%m%d')
        remove_redundancy_df['Date']=remove_redundancy_df['Date'].dt.strftime('%Y-%m-%d')
        return remove_redundancy_df.to_dict(orient="records") , ['Level']

    @staticmethod
    def margin_trading_cleaner(df:pd.DataFrame):
        name_mapping = {
          '股票代號':'stock_id',
          '股票名稱':'stock_name',
          '融資前日餘額':"MarginPurchaseYesterdayBalance",
          '融資買進':"MarginPurchaseBuy",
          '融資賣出':"MarginPurchaseSell",
          '融資現金償還':"MarginPurchaseCashRepayment",
          '融資今日餘額':"MarginPurchaseTodayBalance",
          '融資限額':'MarginPurchaseLimit',
          '融券前日餘額':'ShortSaleYesterdayBalance',
          '融券賣出':'ShortSaleSell',
          '融券買進':"ShortSaleBuy",
          '融券現券償還':'ShortSaleCashRepayment',
          '融券今日餘額': "ShortSaleTodayBalance",
          '融券限額': "ShortSaleLimit",
          '資券互抵':'OffsetLoanAndShort',
          '註記':'Note'}

        new_cols = list(name_mapping.values())
        renamed_df = rename_col(df,name_mapping)
        renamed_df[new_cols[2:-1]] =renamed_df[new_cols[2:-1]].astype(int)

        return renamed_df

    def Listed_CompaniesNews(self,df:pd.DataFrame):
        return  rename_col(df,get_company_news_col())

class TWOTC_opendata_cleaner:
    def __init__(self):
        self.twotc_stock_col = {
            "代號": "StockID",
            "名稱": "Name",
            "收盤": "Close",
            "漲跌": "Change",
            "開盤": "Open",
            "最高": "Max",
            "最低": "Min",
            "成交股數": "TradeVolume",
            "成交筆數": "Transaction",
            "成交金額": "TradeValue",
        }

    def OTC_Day_Transaction_Info(self, df: pd.DataFrame) -> List[dict]:
        rename_col_df = rename_col(df, self.twotc_stock_col)
        clean_df = df_cleaner(rename_col_df)
        return clean_df.to_dict(orient="records") , []

    @staticmethod
    def margin_trading_cleaner(df:pd.DataFrame):
        name_mapping = {
              '資料日期':'date',
              '代號':'stock_id',
              '名稱':'stock_name',
              '前資餘額':"MarginPurchaseYesterdayBalance",
              '資買':"MarginPurchaseBuy",
              '資賣':"MarginPurchaseSell",
              '現償':"MarginPurchaseCashRepayment",
              '資餘額':"MarginPurchaseTodayBalance",
              '資限額':'MarginPurchaseLimit',
              '前券餘額':'ShortSaleYesterdayBalance',
              '券賣':'ShortSaleSell',
              '券買':"ShortSaleBuy",
              '券償':'ShortSaleCashRepayment',
              '券餘額':"ShortSaleTodayBalance",
              '券限額':"ShortSaleLimit",
              '資券相抵':'OffsetLoanAndShort',
              '備註':'Note'}

        new_cols = list(name_mapping.values())
        renamed_df = rename_col(df,name_mapping)
        renamed_df[new_cols[3:-1]] =renamed_df[new_cols[3:-1]].astype(int)

        renamed_df['date']=renamed_df['date'].apply(turn_year)
        renamed_df['date'] = pd.to_datetime(renamed_df['date']).dt.strftime('%Y-%m-%d')

        return renamed_df

    def OTC_CompaniesNews(self,df:pd.DataFrame):
        return  rename_col(df,get_company_news_col())


def turntoint(s: str) -> str:
    if not isinstance(s, str):
        raise TypeError(f"{s} must be a string")

    new = s.replace(",", "")

    if not (new[0].isnumeric() and new[-1].isnumeric()):
        new = "0"

    return new


class TWOther_cleaner:
    def __init__(self):
        self.res = []
        self.day_trade_header_mapping = {
            "證券代號": "StockID",
            "證券名稱": "Name",
            "暫停現股賣出後現款買進當沖註記": "BuyAfterSale",
            "當日沖銷交易成交股數": "DayTradeVolume",
            "當日沖銷交易買進成交金額": "DayTradeBuyAmount",
            "當日沖銷交易賣出成交金額": "DayTradeSellAmount",
        }

    def All_Day_Trade(self, response) -> List[dict]:
        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all("table")

        for table in [tables[1], tables[3]]:
            self.parse_day_trade_table(table)

        return self.res, []

    def parse_day_trade_table(self, table):
        headers = []
        tr_elements = table.find_all("tr")

        for i in range(len(tr_elements)):
            if not tr_elements[i].find("td"):
                tags = "th"
                header_line = tr_elements[i].find(tags)
            else:
                tags = "td"
                header_line = tr_elements[i].find(tags)

            if header_line.text == "證券代號":
                headers = [
                    self.day_trade_header_mapping[header.text]
                    for header in tr_elements[i].find_all(tags)
                ]
                start_point_alltr = i
                break

        if headers == []:
            raise ValueError("Can't find the header line")
            return None

        for i, n in enumerate(table.find_all("tr")[start_point_alltr + 1 :]):
            required_data = {}
            for header, data in zip(headers, n.find_all("td")):
                value = data.text

                if header in [
                    "DayTradeVolume",
                    "DayTradeBuyAmount",
                    "DayTradeSellAmount",
                ]:
                    new_data = turntoint(value)
                else:
                    new_data = value.replace(r" ", "")

                required_data[header] = new_data

            self.res.append(required_data)


