import requests
import pandas as pd
from datetime import datetime

def parse_data(response) -> pd.DataFrame:
    return pd.read_xml(response.content, xpath='.//Record')


def prepare_date_format(date):
    return date.date().strftime("%d-%m-%Y")


def assign_date_column(df):
    def assign_datetime_column(df):
        until_cols = [col for col in df.columns if 'PERIOD_UNTIL' in col]
        time_col = [col for col in df.columns if 'TIME' in col]
        if len(until_cols) == 1:
            df['DATETIME'] = pd.to_datetime(df.DATE.astype(str) + ' ' + df[until_cols[0]])
        elif time_col == 1:
            df['DATETIME'] = pd.to_datetime(df.DATE.astype(str) + ' ' + df[time_col[0]])
        else:
            df['HOUR'] = df.PTU // 4
            df['MINUTE'] = df.PTU % 4 * 15
            df.HOUR = df.HOUR.replace(24, 0)
            df['DATETIME'] = pd.to_datetime(
                df.DATE.astype(str) + ' ' + df.HOUR.astype(str) + ':' + df.MINUTE.astype(str))
            df.drop(columns=['HOUR', 'MINUTE'])
        df.DATETIME = df.DATETIME.dt.tz_localize('CET', ambiguous='infer', nonexistent='shift_forward')
        df.DATETIME = df.DATETIME.mask((df.DATETIME.dt.hour == 0) & (df.DATETIME.dt.minute == 0),
                                       df.DATETIME + pd.Timedelta(days=1))
        return df.set_index("DATETIME")

    df.DATE = pd.to_datetime(df.DATE)
    return assign_datetime_column(df)


class TenneTClient:

    def __init__(self):
        self.base_url = 'http://www.tennet.org/english/operational_management/export_data.aspx?'

    def _api_call(self, url_addition):
        response = requests.get(url_addition)
        response.raise_for_status()
        return response

    def _obtain_data_from_website(self, url) -> pd.DataFrame:
        return assign_date_column(parse_data(self._api_call(self.base_url + url)))

    def _uri_addition(self, export_type, start_date, end_date):
        return f'exporttype={export_type}&' \
               f'format=xml&' \
               f'datefrom={prepare_date_format(start_date)}&' \
               f'dateto={prepare_date_format(end_date)}&' \
               f'submit=1'

    def query_available_capacity(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        """
        The total available production capacity known by TenneT
        :param start_date: The first date to obtain the data from
        :param end_date: The last date to obtain the data from
        :return:
        """
        return self._obtain_data_from_website(self._uri_addition('availablecapacity', start_date, end_date))

    def query_balance_delta_prices(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        """
        The activated reserve capacity, including the prices, on a per-minute basis.
        The data does not contain the intraday balancing information.
        :param start_date: The first date to obtain the data from
        :param end_date: The last date to obtain the data from
        :return:
        """
        return self._obtain_data_from_website(self._uri_addition('BalansdeltaPrices', start_date, end_date))

    def query_balance_delta_igcc(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        """
        The activated reserve capacity, including the prices and cross-border balancing, on a per-minute basis.
        The data does not contain the intraday balancing information.
        :param start_date: The first date to obtain the data from
        :param end_date: The last date to obtain the data from
        :return:
        """
        return self._obtain_data_from_website(self._uri_addition('balancedeltaIGCC', start_date, end_date))

    def query_bid_price_ladder(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        """
        The bid ladder for the reserve capacity
        :param start_date: The first date to obtain the data from
        :param end_date: The last date to obtain the data from
        :return:
        """
        return self._obtain_data_from_website(self._uri_addition('bidpriceladder', start_date, end_date))

    def query_ladder_size(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        """
        The reserve capacity ladder size
        :param start_date: The first date to obtain the data from
        :param end_date: The last date to obtain the data from
        :return:
        """
        return self._obtain_data_from_website(self._uri_addition('laddersize15', start_date, end_date))

    def query_history_deployed_capacity(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        """
        THe deployed reserve capacity
        :param start_date: The first date to obtain the data from
        :param end_date: The last date to obtain the data from
        :return:
        """
        return self._obtain_data_from_website(self._uri_addition('volumerrecapacity', start_date, end_date))

    def query_settled_imbalance(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        """
        The settled imbalance
        :param start_date: The first date to obtain the data from
        :param end_date: The last date to obtain the data from
        :return:
        """
        return self._obtain_data_from_website(self._uri_addition('volumesettledimbalance', start_date, end_date))

    def query_intraday(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        """

        :param start_date: The first date to obtain the data from
        :param end_date: The last date to obtain the data from
        :return:
        """
        return self._obtain_data_from_website(self._uri_addition('intraday', start_date, end_date))

    def query_imbalance(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        """
        The daily imbalance data
        :param start_date: The first date to obtain the data from
        :param end_date: The last date to obtain the data from
        :return:
        """
        return self._obtain_data_from_website(self._uri_addition('imbalance', start_date, end_date))

    def query_settlement_prices(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        """
        The secondary reserve price
        :param start_date: The first date to obtain the data from
        :param end_date: The last date to obtain the data from
        :return:
        """
        return self._obtain_data_from_website(self._uri_addition('settlementprices', start_date, end_date))

    def query_installed_capacity(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        """

        :param start_date: The first date to obtain the data from
        :param end_date: The last date to obtain the data from
        :return:
        """
        return self._obtain_data_from_website(self._uri_addition('installedcapacity', start_date, end_date))

    def query_measurement_data(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        """
        The feed to the grid and the balance of exchanges to other countries
        :param start_date: The first date to obtain the data from
        :param end_date: The last date to obtain the data from
        :return:
        """
        return self._obtain_data_from_website(self._uri_addition('measurementdata', start_date, end_date))

    def query_regulating_margin(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        """
        The available capacity minus the capacity deployed
        :param start_date: The first date to obtain the data from
        :param end_date: The last date to obtain the data from
        :return:
        """
        return self._obtain_data_from_website(self._uri_addition('regulatingmargin', start_date, end_date))

    def query_thirty_days_ahead(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        """

        :param start_date: The first date to obtain the data from
        :param end_date: The last date to obtain the data from
        :return:
        """
        return self._obtain_data_from_website(self._uri_addition('thirtydaysahead', start_date, end_date))

    def query_laddersize_total(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        """
        Obtain the total available secondary capacity
        :param start_date: The first date to obtain the data from
        :param end_date: The last date to obtain the data from
        :return:
        """
        return self._obtain_data_from_website(self._uri_addition('laddersizetotal', start_date, end_date))

    def query_actual_imbalance(self):
        """ Obtain the actual imbalance with a delay of 2 minutes"""
        response = self._api_call('https://www.tennet.org/xml/balancedeltaprices/balans-delta.xml')
        df = pd.read_xml(response.content)
        df['DATETIME'] = pd.to_datetime(datetime.today().date().strftime('%Y-%m-%d') + ' ' +  df.TIME).dt.tz_localize('CET', ambiguous='infer', nonexistent='shift_forward')
        return df.set_index('DATETIME')

