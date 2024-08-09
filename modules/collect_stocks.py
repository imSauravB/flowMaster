from urllib import request
from yfinance import Ticker
from typing import Dict
from pprint import pprint
from datetime import datetime
import requests
from utils import save_data

ticker_list = ['APPL', 'MSFT', 'AMZN', 'META', 'NTFX', 'GOOG']

current_date = datetime.now().strftime("%Y%m%d")

def collect_stock_data(stock_ticker: str):
    try:
        ticker_data = Ticker(stock_ticker)
        df_ticker_data = ticker_data.history('1mo')
        if not df_ticker_data.empty:
            pprint(df_ticker_data)
            df_ticker_data.reset_index(inplace=True)
            return df_ticker_data
        else:
            print(f"Error: Could not retrieve the latest data for {stock_ticker}.")
            return None
    except Exception as e:
        print(f"Failed to fetch data for {stock}: {e}")
        return None

def get_top_100_stocks():
    try:
        headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
        }
        res = requests.get("https://api.nasdaq.com/api/quote/list-type/nasdaq100", headers=headers)
        main_data = res.json().get('data', {}).get('data', {}).get('rows', [])
        stock_symbols = [item['symbol'] for item in main_data]
        return stock_symbols
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

for ticker in ticker_list:
    stock_data = collect_stock_data(ticker)
    if stock_data is not None:
        file_name = f"{current_date}_stock_{ticker}"
        save_data(stock_data, file_name, zone="raw", context="stocks", file_type="csv")
