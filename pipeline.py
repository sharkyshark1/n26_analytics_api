import os
from database import Database
from n26_api import N26Caller
from processing import TransactionProcessor
import dotenv
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import use
use('Agg')
# import datetime
import io
import zipfile


dotenv.load_dotenv(".env", verbose=True)

from utils import Utils


class Pipeline:
    def __init__(self):
        self.name = "I am the processing pipeline"
        self.n26 = N26Caller()
        self.storage = Database()
        self.processor = TransactionProcessor()

    def update_data(self):
        document_count = self.storage.get_document_count()
        print('DOC COUNT', document_count)
        if document_count == 0:
            incoming_transactions = self.n26.get_all_transactions()
            self.storage.insert_multiple_transactions(transaction_list=incoming_transactions)
        else:
            time_last_db = self.storage.get_last_transaction_time()
            print('TIME OF LAST STORED TRANSACTION', time_last_db)
            incoming_transactions = self.n26.get_recent_transactions(start_time=time_last_db)
            self.storage.insert_multiple_transactions(transaction_list=incoming_transactions)

    def add_date_columns(self, df):
        df['year'] = pd.DatetimeIndex(df['created_ts_dt']).year
        df['month'] = pd.DatetimeIndex(df['created_ts_dt']).month
        df['week'] = pd.DatetimeIndex(df['created_ts_dt']).week
        df['day'] = pd.DatetimeIndex(df['created_ts_dt']).day
        df = df.set_index(pd.DatetimeIndex(df['created_ts_dt']))
        return df

    def strip_spaces(self, df):
        return df[df.payment_scheme != 'SPACES']

    def get_csv(self, df):
        return df.to_csv(sep=';', encoding='UTF-8')

    def get_expenditures(self, df):
        return df[(df.amount < 0) & (df.partner_name != 'Tjark Petersen')]

    def get_savings(self, df):
        # issue: holidays booked via savings account :/
        return df[(df.amount < 0) & (df.partner_name == 'Tjark Petersen')]

    def get_atm(self, df):
        return df[df.category == 'micro-v2-atm']

    def estimate_savings_rate(self, df):
        s_df = self.get_savings(df)
        print(s_df.head())
        # first = datetime.date.today().replace(day=1)
        # last_month = first - datetime.timedelta(days=1)
        savings_df = s_df.amount.resample('M', how='sum')
        # savings_df = s_df[(datetime.date(s_df['created_ts_dt']) >= datetime.date(2019, 10, 1)) &
        #                  (datetime.date(s_df['created_ts_dt']) <= last_month)]
        savings_monthly = savings_df
        return savings_monthly

    def get_time_series(self, df, aggregation_level, graph, start):
        data = self.get_expenditures(df).sort_index(axis=0)
        start_date = pd.to_datetime(start)
        data = data[data.index >= start_date]
        result = data.resample(aggregation_level).amount.sum() * -1
        ax = result.plot(kind=graph, figsize=(20, 10))
        ax.set_xlabel("time")
        ax.set_ylabel("expenditures")
        if graph == 'bar':
            ax.set_xticklabels([str(j.day) + '-' + str(j.month) + '-' + str(j.year) for j in result.index])
        bytes_image = io.BytesIO()
        plt.savefig(bytes_image, format='png')
        bytes_image.seek(0)
        return bytes_image

    def get_zip_file(self, files):
        mem_zip = io.BytesIO()
        with zipfile.ZipFile(mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            i = 0
            for f in files:
                zf.writestr(str(i) + 'blabla.png', f.getvalue())
                i+=1
        mem_zip.seek(0)
        return mem_zip

    def run(self):

        ''''
        rewe = df[df.merchant_name.str.contains('REWE')].sort_values(by='created_ts_dt', ascending=False)
        '''


        print('Number of transactions currently persisted: ', self.storage.get_document_count())
        # self.update_data()
        print('Number of transactions currently persisted: ', self.storage.get_document_count())
        df = self.processor.get_transactions_df()
        df = self.add_date_columns(df)
        df = self.strip_spaces(df)

        # define outcomes
        p = self.get_time_series(df=df, aggregation_level='1M', graph='bar', start='01-01-2020')
        p2 = self.get_time_series(df=df, aggregation_level='1M', graph='bar', start='01-01-2020')
        zip_file = self.get_zip_file(files=[p, p2])
        return zip_file


# Pipeline().run()