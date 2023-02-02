import pandas as pd


class DataFrame:

    @staticmethod
    def get_raw_data(api: str):
        try:
            data = pd.read_csv(api)
        except FileNotFoundError:
            data = pd.DataFrame()

        return data
    