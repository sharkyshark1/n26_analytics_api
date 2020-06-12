from database import Database
from utils import Utils
import pandas as pd


class TransactionProcessor:
    def __init__(self):
        self.name = 'I am a data processor.'
        self.dict_columns = Utils().dict_columns
        self.columns = Utils().columns
        # the path is for test purposes
        self.df_path = '/Users/sharkyshark6/Desktop/n26_df/n26_processed_df.csv'

    @staticmethod
    def transform_instance(instance):
        created_ts = instance.get('createdTS')
        created_ts_dt = Utils().convert_utc_datetime(timestamp=created_ts)
        instance['createdTS_DT'] = created_ts_dt
        return instance

    def get_transactions_df(self):
        processed_transactions = list()
        raw_transactions = Database().get_all_transactions()
        for transaction in raw_transactions:
            modified = self.transform_instance(instance=transaction)
            keys = modified.keys()
            processed_transaction = [modified.get(k) if (k in keys) else '-' for k in self.dict_columns]
            processed_transactions.append(processed_transaction)
        df = pd.DataFrame(processed_transactions, columns=self.columns)
        return df

    def save_df(self, df):
        df.to_csv(path_or_buf=self.df_path, sep=';', encoding='UTF-8')




