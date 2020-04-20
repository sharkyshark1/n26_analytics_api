import os
from database import Database
from n26_api import N26Caller
from processing import TransactionProcessor
# solve later on!
# from dotenv import load_dotenv()
# load_dotenv()
# os.environ['TEST'] = 'bla'
# print(os.getenv('TEST'))
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

    def run(self) -> None:
        # self.n26.get_balance()
        # self.n26.test()

        self.storage.delete_transactions()
        self.update_data()

        # self.storage.insert_transaction(transaction=self.storage.example)
        print(self.storage.get_document_count())
        df = self.processor.get_transactions_df()
        self.processor.save_df(df=df)







Pipeline().run()