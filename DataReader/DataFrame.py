import pandas as pd


class DataFrame:
    @staticmethod
    def get_raw_data(api: str,**kwargs):
        try:
            data = pd.read_csv(api,**kwargs)
        except FileNotFoundError:
            data = pd.DataFrame()

        return data
