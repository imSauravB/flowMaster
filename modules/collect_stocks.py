from yfinance import Ticker
from typing import Dict
from pprint import pprint
from datetime import datetime
from utils import save_data

ticker_list = ['APPL', 'MSFT', 'AMZN', 'META', 'NTFX', 'GOOG']

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
    except:
        return

current_date = datetime.now().strftime("%Y%m%d")

for ticker in ticker_list:
    stock_data = collect_stock_data(ticker)
    if stock_data is not None:
        file_name = f"{current_date}_stock_{ticker}"
        save_data(stock_data, file_name, zone="raw", context="stocks", file_type="csv")
