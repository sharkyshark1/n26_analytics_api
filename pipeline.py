import io
import zipfile
from database import Database
from n26_api import N26Caller
from processing import TransactionProcessor
import dotenv
import pandas as pd
import datetime
import matplotlib.pyplot as plt
from matplotlib import use
use('Agg')

dotenv.load_dotenv(".env", verbose=True)


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

    @staticmethod
    def add_date_columns(df):
        df['year'] = pd.DatetimeIndex(df['created_ts_dt']).year
        df['month'] = pd.DatetimeIndex(df['created_ts_dt']).month
        df['week'] = pd.DatetimeIndex(df['created_ts_dt']).week
        df['day'] = pd.DatetimeIndex(df['created_ts_dt']).day
        df = df.set_index(pd.DatetimeIndex(df['created_ts_dt']))
        return df

    @staticmethod
    def strip_spaces(df):
        return df[df.payment_scheme != 'SPACES']

    @staticmethod
    def get_csv(df):
        return df.to_csv(sep=';', encoding='UTF-8')

    @staticmethod
    def get_expenditures(df):
        return df[(df.amount < 0) & (df.partner_name != 'Tjark Petersen')]

    @staticmethod
    def get_savings(df):
        # issue: holidays booked via savings account :/
        return df[(df.amount < 0) & (df.partner_name == 'Tjark Petersen')]

    @staticmethod
    def get_atm(df):
        return df[df.category == 'micro-v2-atm']

    def get_time_series(self, df, aggregation_level, graph, start, add_name=None):
        data = self.get_expenditures(df).sort_index(axis=0)
        start_date = pd.to_datetime(start)
        data = data[data.index >= start_date]
        result = data.resample(aggregation_level).amount.sum() * -1

        fname = 'expenditures_' + aggregation_level + '_' + graph + '_' + str(start)
        if add_name:
            fname = fname + '-' + add_name + '.png'
        else:
            fname = fname + '.png'

        plt.figure()
        ax = result.plot(kind=graph, figsize=(20, 10))
        ax.set_xlabel("time")
        ax.set_ylabel("expenditures")
        if graph == 'bar':
            ax.set_xticklabels([str(j.day) + '-' + str(j.month) + '-' + str(j.year) for j in result.index])
        bytes_image = io.BytesIO()
        plt.savefig(bytes_image, format='png', bbox_inches='tight')
        bytes_image.seek(0)
        return [fname, bytes_image]

    @staticmethod
    def filter_by_string(df, search_string):
        df_filtered = df[df.merchant_name.str.contains(search_string)]
        if df_filtered.shape[0] == 0:
            df_filtered = df[df.partner_name.str.contains(search_string)]
        return df_filtered

    @staticmethod
    def filter_by_category(df, category_string):
        df_filtered = df[df.category.str.contains(category_string)]
        return df_filtered

    def plot_category_mix(self, df, start, end):
        data = self.get_expenditures(df).sort_index(axis=0)
        start_date = pd.to_datetime(start)
        end_date = pd.to_datetime(end)
        data = data[(data.index >= start_date) & (data.index < end_date)]
        result = data.groupby('category').amount.sum()/data.amount.sum()
        result = result.to_frame().reset_index().set_index('category')

        fname = 'exp_category_mix_' + str(start) + '_' + str(end) + '.png'
        plt.figure()
        ax = result.plot(kind='bar', figsize=(20, 10))
        ax.set_xlabel("category")
        ax.set_ylabel("expenditures")
        bytes_image = io.BytesIO()
        plt.savefig(bytes_image, format='png', bbox_inches='tight')
        bytes_image.seek(0)
        return [fname, bytes_image]

    @staticmethod
    def get_zip_file(files):
        mem_zip = io.BytesIO()
        with zipfile.ZipFile(mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            i = 0
            for f in files:
                zf.writestr(f[0], f[1].getvalue())
                i += 1
        mem_zip.seek(0)
        return mem_zip

    def run(self):

        print('Number of transactions currently persisted: ', self.storage.get_document_count())
        self.update_data()
        print('Number of transactions currently persisted: ', self.storage.get_document_count())
        df = self.processor.get_transactions_df()
        df = self.add_date_columns(df=df)
        df = self.strip_spaces(df=df)

        # define outcomes and add to the list

        today = datetime.date.today()
        start_this_month = today.replace(day=1)
        start_last_month = start_this_month.replace(month=today.month - 1)

        plot_0 = self.get_time_series(df=df, aggregation_level='1M', graph='bar', start='01-01-2020')
        plot_1 = self.get_time_series(df=df, aggregation_level='1M', graph='line', start='01-01-2020')
        plot_2 = self.get_time_series(df=df, aggregation_level='1W', graph='line', start='01-01-2020')
        plot_3 = self.get_time_series(df=df, aggregation_level='1D', graph='line', start='01-01-2020')
        plot_4 = self.get_time_series(df=df, aggregation_level='1M', graph='line', start='01-01-2016')
        plot_5 = self.get_time_series(df=df, aggregation_level='1D', graph='line', start=start_last_month)

        plot_6 = self.plot_category_mix(df=df, start=start_last_month, end=start_this_month)
        plot_7 = self.plot_category_mix(df=df, start='01-01-2020', end=today)

        df_supermarket = self.filter_by_category(df=df, category_string='micro-v2-food-groceries')
        plot_8 = self.get_time_series(df=df_supermarket, aggregation_level='1M', graph='bar', start='10-01-2019', add_name='SUPERMARKET')
        plot_9 = self.get_time_series(df=df_supermarket, aggregation_level='1W', graph='line', start='10-01-2019', add_name='SUPERMARKET')
        plot_10 = self.get_time_series(df=df_supermarket, aggregation_level='1D', graph='line', start='10-01-2019', add_name='SUPERMARKET')

        df_travel_transport = self.filter_by_category(df=df, category_string='micro-v2-travel-holidays')
        plot_8 = self.get_time_series(df=df_travel_transport, aggregation_level='1D', graph='line', start='10-01-2019',
                                      add_name='TRAVEL_TRANSPORT')

        df_rewe = self.filter_by_string(df=df, search_string='REWE')
        plot_11 = self.get_time_series(df=df_rewe, aggregation_level='1W', graph='line', start='10-01-2019', add_name='REWE')

        df_electricity = self.filter_by_string(df=df, search_string='vivi-power GmbH')
        plot_12 = self.get_time_series(df=df_electricity, aggregation_level='1M', graph='bar', start='10-01-2019', add_name='ELECTRICITY')

        output_files = [plot_0, plot_1, plot_2, plot_3, plot_4, plot_5, plot_6, plot_7, plot_8, plot_9, plot_10,
                        plot_11, plot_12]
        zip_file = self.get_zip_file(files=output_files)
        return zip_file

# Pipeline().run()

