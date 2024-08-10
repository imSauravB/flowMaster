import os
import sys
sys.path.append('/opt/airflow')
from argparse import ArgumentParser
import pandas as pd
from datetime import datetime
import configs.constants as Constants
from utils.utils import save_data
import time

processed_stocks = []

def dataPreProcessing(data_lake_path: str):
    for dir in os.listdir(os.path.join(data_lake_path,Constants.RAW_STOCKS_FOLDER_PATH)):
        for file_name in os.listdir( os.path.join(data_lake_path,Constants.RAW_STOCKS_FOLDER_PATH, dir) ):
            file_path = os.path.join(data_lake_path, Constants.RAW_STOCKS_FOLDER_PATH, dir, file_name)
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

        time.sleep(2)
        print("going to save_data")

    final_stocks_df = pd.concat(processed_stocks, ignore_index=True)
    save_data(final_stocks_df, base_path=data_lake_path, file_name="stocks", zone="refined", context="stocks", file_type="parquet")

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--data_lake_path", required=True, help="Path to the data lake")
    args = parser.parse_args()
    data_lake_path = args.data_lake_path
    dataPreProcessing(data_lake_path)