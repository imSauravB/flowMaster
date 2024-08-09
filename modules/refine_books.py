import os
import json
import pandas as pd
from utils import save_data
from pprint import pprint
from datetime import datetime

RAW_FOLDER_PATH = "./data/raw/books"

processed_books = []

for file_name in os.listdir(RAW_FOLDER_PATH):
    file_path = os.path.join(RAW_FOLDER_PATH, file_name)
    print(f"inside for loop, reading file {file_path}")
    with open(file_path, 'r') as f:
        book_data = json.load(f)
    processed_book = {
        "title": book_data.get("title", None),
        "subtitle": book_data.get("subtitle", None),
        "number_of_pages": book_data.get("number_of_pages", None),
        "publish_date": book_data.get("publish_date", None),
        "publish_country": book_data.get("publish_country", None),
        "by_statement": book_data.get("by_statement", None),
        "publish_places": "|".join(book_data.get("publish_places", [])),
        "publishers": "|".join(book_data.get("publishers", [])),
        "authors_uri": "|".join(book_data.get("authors_uri", [])),
        "id": book_data.get("key", str.split(file_name, '_')[2]),
        "collect_date": datetime.strptime(str.split(file_name, '_')[0], '%Y%m%d').strftime('%Y-%m-%d')
    }
    processed_books.append(processed_book)

df_books = pd.DataFrame(processed_books)
df_books.drop_duplicates(subset=['id'], keep='last', inplace=True)

result = save_data(df_books.to_dict(orient='records'), "books", "parquet", "refined")
print(result)

df_verify_book = pd.read_parquet("./data/refined/books/books.parquet")
print(df_verify_book)
print("Columns:", df_verify_book.columns.tolist())