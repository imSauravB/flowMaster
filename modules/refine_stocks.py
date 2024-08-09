import os
import json
import pandas as pd
from utils import save_data
from pprint import pprint
from datetime import datetime


RAW_FOLDER_PATH = "./data/raw/stocks"
PROCESSED_FOLDER_PATH = "./data/refined/stocks"

processed_stocks = []

for file_name in os.listdir(RAW_FOLDER_PATH):

    file_path = os.path.join(RAW_FOLDER_PATH, file_name)
    print(f"inside for loop, reading file {file_path}")
    df_stocks = pd.read_csv(file_path)

    df_stocks['Date'] = pd.to_datetime(df_stocks['Date']).dt.strftime('%Y-%m-%d')
    df_stocks['Open'] = df_stocks['Open'].astype('float64')
    df_stocks['High'] = df_stocks['High'].astype('float64')
    df_stocks['Low'] = df_stocks['Low'].astype('float64')
    df_stocks['Close'] = df_stocks['Close'].astype('float64')
    df_stocks['Volume'] = df_stocks['Volume'].astype('int64')
    df_stocks['ticker_name'] = str.split(file_name, '_')[2]
    df_stocks['collect_date'] = datetime.strptime(str.split(file_name, '_')[0], '%Y%m%d').strftime('%Y-%m-%d')

    processed_stocks.append(df_stocks)

    final_stocks_df = pd.concat(processed_stocks, ignore_index=True)

    save_data(final_stocks_df, file_name="stocks", zone="refined", context="stocks", file_type="parquet")

    df_parquet = pd.read_parquet(f"{PROCESSED_FOLDER_PATH}/stocks.parquet")
    print("Printing parquet file...")
    print(df_parquet)