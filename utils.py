import datetime


class Utils:
    def __init__(self):
        self.name = 'Utils Class'
        self.epoch = datetime.datetime(1970, 1, 1, 0, 0, 0)
        self.columns = ['transaction_id', 'transaction_type', 'amount', 'created_ts', 'created_ts_dt', 'merchant_name',
                        'merchant_city', 'merchant_country', 'category', 'currency', 'original_amount',
                        'original_currency', 'exchange_rate', 'txn_condition', 'recurrent', 'partner_name',
                        'partner_iban', 'payment_scheme']

        self.dict_columns = ['id', 'type', 'amount', 'createdTS', 'createdTS_DT', 'merchantName', 'merchantCity',
                             'merchantCountry', 'category', 'currencyCode', 'originalAmount', 'originalCurrency',
                             'exchangeRate', 'txnCondition', 'recurrent', 'partnerName', 'partnerIban', 'paymentScheme']

    def get_current_timestamp(self):
        now = datetime.datetime.utcnow()
        timestamp_now = int((now - self.epoch).total_seconds() * 1000)
        return timestamp_now

    @staticmethod
    def convert_utc_datetime(timestamp):
        timestamp = timestamp / 1000
        dt = datetime.datetime.fromtimestamp(timestamp).strftime('%m-%d-%Y %H:%M:%S')
        return dt

