from pymongo import MongoClient
from passlib.hash import pbkdf2_sha256


class Database:
    def __init__(self):
        self.name = 'This is the Mongo DB'
        self.client = MongoClient()
        self.db = self.client.banking_database
        self.collection = self.db.banking_collection
        self.collection_users = self.db.users

        self.example = {'id': '12db367d-ed33-40cf-91ed-071f5a591651', 'userId': '812f156b-81bd-4c7b-8557-d4ff89138f10',
                        'type': 'AA', 'amount': -5.27, 'currencyCode': 'EUR', 'originalAmount': -5.27,
                        'originalCurrency': 'EUR', 'exchangeRate': 1.0, 'merchantCity': 'Berlin',
                        'visibleTS': 1587211163000, 'mcc': 5411, 'mccGroup': 7, 'merchantName': 'REWE Markt GmbH-Zw',
                        'recurring': False, 'partnerAccountIsSepa': False,
                        'accountId': '3a83b796-90dc-4b3b-882b-0b53bb2792da', 'category': 'micro-v2-food-groceries',
                        'cardId': '5754968a-e9a7-4e95-aa8c-2f21cfc8a6a3', 'userCertified': 1587211163077,
                        'pending': False, 'transactionNature': 'NORMAL', 'createdTS': 1587211163077,
                        'merchantCountry': 0, 'merchantCountryCode': 276, 'txnCondition': 'CARD_PRESENT',
                        'smartLinkId': '12db367d-ed33-40cf-91ed-071f5a591651',
                        'linkId': '12db367d-ed33-40cf-91ed-071f5a591651', 'confirmed': 1587211163077}

    def insert_transaction(self, transaction):
        post_id = self.collection.insert_one(transaction).inserted_id

    def insert_multiple_transactions(self, transaction_list):
        result = self.collection.insert_many(transaction_list)

    def get_all_transactions(self):
        transactions = list(self.collection.find({}))
        return transactions

    def get_transactions_by_attribute(self, attributes):
        transactions = list(self.collection.find_one(attributes))
        return transactions

    def delete_transactions(self):
        self.collection.remove()
        print('Removed all transactions in the collection.')

    def get_last_transaction(self):
        last_transaction = self.collection.find_one(sort=[('createdTS', -1)])
        return last_transaction

    def get_last_transaction_time(self):
        last_transaction = self.get_last_transaction()
        time = last_transaction.get('createdTS')
        return time

    def get_document_count(self):
        return self.collection.count()

    def verify_user(self, selfusername, password):
        user_obj = self.collection_users.find_one({'username': selfusername})
        if user_obj is None:
            return None
        if not pbkdf2_sha256.verify(password, user_obj['pwhash']):
            return None
        return user_obj



