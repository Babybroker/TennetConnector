from tennet.helpers.DataQueries import DataQueries


class TenneTClient:
    def __init__(self):
        self.DQ = DataQueries()

    def query_actual_balance(self):
        balance_data = self.DQ.query_actual_balance()