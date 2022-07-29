import requests
import pandas as pd


def parse_data(response) -> pd.DataFrame:
    return pd.read_xml(response.content)


def prepare_date_format(date):
    return date.date().strftime("%Y%m%d")


class DataQueries:
    def __init__(self):
        self.base_url = 'https://www.tennet.org/xml/'

    def api_call(self, url_addition):
        response = requests.get(self.base_url + url_addition)
        response.raise_for_status()
        return response

    def obtain_data_from_website(self, url_addition) -> pd.DataFrame:
        return parse_data(self.api_call(url_addition))

    def query_actual_balance(self) -> pd.DataFrame:
        return self.obtain_data_from_website('balancedeltaprices/balans-delta.xml')

    def query_ladder_size(self, date: pd.Timestamp) -> pd.DataFrame:
        return self.obtain_data_from_website(f'laddersize15/{prepare_date_format(date)}.xml')

    def query_available_afrr_capacity(self, date: pd.Timestamp) -> pd.DataFrame:
        return self.obtain_data_from_website(f'brov/{prepare_date_format(date)}.xml')

    def query_offered_afrr_capacity(self, date: pd.Timestamp) -> pd.DataFrame:
        return self.obtain_data_from_website(f'arov/{prepare_date_format(date)}.xml')

    def query_bid_price_ladder(self, date: pd.Timestamp) -> pd.DataFrame:
        return self.obtain_data_from_website(f'priceladder/{prepare_date_format(date)}.xml')

    def query_total_ladder_size(self, date: pd.Timestamp) -> pd.DataFrame:
        return self.obtain_data_from_website(f'laddersizetotal/{prepare_date_format(date)}.xml')

    def query_settlement_prices(self, date: pd.Timestamp) -> pd.DataFrame:
        return self.obtain_data_from_website(f'imbalanceprice/{prepare_date_format(date)}.xml')
