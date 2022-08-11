import pandas as pd
from tennet.helpers.DataQueries import DataQueries


def data_looper(func, start, end):
    def assign_date_column(df):

        def assign_datetime_column(df):
            if 'PERIOD_UNTIL' in df.columns:
                df['DATETIME'] = pd.to_datetime(df.DATE.astype(str) + ' ' + df.PERIOD_UNTIL)
            else:
                df['HOUR'] = df.PTU // 4
                df['MINUTE'] = df.PTU % 4 * 15
                df['DATETIME'] = pd.to_datetime(
                    df.DATE.astype(str) + ' ' + df.HOUR.astype(str) + ':' + df.MINUTE.astype(str))
                df.drop(columns=['HOUR', 'MINUTE'])
            return df.set_index("DATETIME")

        df.DATE = pd.to_datetime(df.DATE)
        return assign_datetime_column(df)

    data_list = list()
    for date in pd.date_range(start, end):
        data_list.append(func(date))
    return assign_date_column(pd.concat(data_list))


class TenneTClient:
    def __init__(self):
        self.DQ = DataQueries()

    def obtain_actual_balance(self):
        return self.DQ.query_actual_balance()

    def obtain_ladder_sizes(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        return data_looper(func=self.DQ.query_total_ladder_size, start=start_date, end=end_date)

    def obtain_available_afrr_capacity(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        return data_looper(func=self.DQ.query_available_afrr_capacity, start=start_date, end=end_date)

    def obtain_offered_afrr_capacity(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        return data_looper(func=self.DQ.query_offered_afrr_capacity, start=start_date, end=end_date)

    def obtain_bid_price_ladder(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        return data_looper(func=self.DQ.query_bid_price_ladder, start=start_date, end=end_date)

    def obtain_total_ladder_size(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        return data_looper(func=self.DQ.query_total_ladder_size, start=start_date, end=end_date)

    def obtain_settlement_prices(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        return data_looper(func=self.DQ.query_available_afrr_capacity, start=start_date, end=end_date)
