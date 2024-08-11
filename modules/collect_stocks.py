import sys
sys.path.append('/opt/airflow')
print(sys.path)
from urllib import request
from yfinance import Ticker
from pprint import pprint
from datetime import datetime
import requests
from utils.utils import save_data
from argparse import ArgumentParser
import configs.constants as Constants

ticker_list = ['APPL', 'MSFT', 'AMZN', 'META', 'NTFX', 'GOOG']

current_date = datetime.now().strftime("%Y%m%d")

def collect_stock_data(stock_ticker: str):
    try:
        ticker_data = Ticker(stock_ticker)
        df_ticker_data = ticker_data.history('1mo')
        if not df_ticker_data.empty:
            df_ticker_data.reset_index(inplace=True)
            return df_ticker_data
        else:
            print(f"Error: Could not retrieve the latest data for {stock_ticker}.")
            return None
    except Exception as e:
        print(f"Failed to fetch data for {stock_ticker}: {e}")
        return None

def get_top_100_stocks(standalone: str = None , **kwargs):
    try:
        headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
        }
        res = requests.get(Constants.NASDAQ_URL, headers=headers)
        main_data = res.json().get('data', {}).get('data', {}).get('rows', [])
        stock_symbols = [item['symbol'] for item in main_data]
        if standalone is None:
            ti = kwargs['ti']
            ti.xcom_push(key='stock_symbols', value=stock_symbols)
        return stock_symbols
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def collect_stocks(data_lake_path: str = None, **kwargs):
    try:
        stock_symbols= []
        if data_lake_path is None:
            ti = kwargs['ti']
            stock_symbols = ti.xcom_pull(key='stock_symbols')
            data_lake_path = ti.xcom_pull(key='data_lake_path')
        
        if len(stock_symbols) > 0:
            print("Found stocks list in xcom")
        else:
            print("getting stocks list from get_top_100_stocks function")
            stock_symbols=get_top_100_stocks(standalone="True")
        
        for ticker in stock_symbols:
            stock_data = collect_stock_data(ticker)
            if stock_data is not None:
                file_name = f"{current_date}_stock_{ticker}"
                pprint(f'{file_name}')
                save_data(stock_data, file_name, base_path=data_lake_path, zone="raw", context="stocks", file_type=Constants.STOCKS_FILE_FORMAT)
        return True
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

if __name__ == "__main__":
    print("In collect_stocks main()")
    parser = ArgumentParser()
    parser.add_argument("--data_lake_path", required=True, help="Path to the data lake")
    args = parser.parse_args()
    data_lake_path = args.data_lake_path
    collect_stocks(data_lake_path)
