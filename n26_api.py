import os
import datetime
from n26.api import Api
from n26.config import Config
from utils import Utils

class N26Caller:

    def __init__(self):
        self.mfa_type = "app"
        self.conf = Config(validate=False)
        self.conf.USERNAME.value = os.getenv("N26USER")
        self.conf.PASSWORD.value = os.getenv("N26PASSWORD")
        self.conf.LOGIN_DATA_STORE_PATH.value = None
        self.conf.MFA_TYPE.value = self.mfa_type
        self.conf.validate()
        self.n26_client = Api(self.conf)
        self.utils = Utils()

    def get_balance(self):
        data = self.n26_client.get_balance()
        balance = data.get('availableBalance')
        return balance

    def get_all_transactions(self):
        transactions = self.n26_client.get_transactions(limit=10000)
        # default: from_time: int = None, to_time: int = None, limit: int = 10, pending: bool = None,
        # categories: str = None, text_filter: str = None, last_id: str = None
        return transactions

    def get_recent_transactions(self, start_time):
        start_time += 1
        timestamp_now = self.utils.get_current_timestamp()
        transactions = self.n26_client.get_transactions(limit=1000, from_time=start_time, to_time=timestamp_now)
        return transactions


    def other_functions(self):
        '''
        print('addresses')
        print(self.n26_client.get_addresses())

        print('spaces')
        print(self.n26_client.get_spaces())

        print('cards')
        print(self.n26_client.get_cards())

        print('statements')
        print(self.n26_client.get_statements())

        print('categories')
        print(self.n26_client.get_available_categories())

        print('stats')
        print(self.n26_client.get_statistics())
        # default from_time: int = 0, to_time: int = int(time.time()) * 1000)
        '''



