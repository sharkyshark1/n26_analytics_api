from n26_api import N26Caller


class DataIngestion:
    def __init__(self):
        self.name = 'I am the Data Ingestor'
        self.n26_client = N26Caller().client
        print(self.n26_client)

    def get_balance(self):
        data = self.n26_client.get_balance()
        balance = data.get('availableBalance')
        return balance

    def test(self):
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

        print('transactions')
        print(self.n26_client.get_transactions())
        # default: from_time: int = None, to_time: int = None, limit: int = 10, pending: bool = None,
        # categories: str = None, text_filter: str = None, last_id: str = None




