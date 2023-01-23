import pandas as pd

class DataFrame:
    def get_raw_data(self,api:str):
        return pd.read_csv(api)