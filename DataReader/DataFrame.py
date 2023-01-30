import pandas as pd


class DataFrame:

    @staticmethod
    def get_raw_data(api: str):
        return pd.read_csv(api)
    