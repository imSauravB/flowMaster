import requests
from typing import Dict
from pprint import pprint
from datetime import datetime
from utils import save_data

OPEN_LIBRARY_BASE_URL = "https://openlibrary.org/works/"

ERROR = "Error"
FILE_FORMAT = ".json"

books_list = ['OL47317227M', 'OL38631342M', 'OL46057997M', 'OL26974419M', 'OL10737970M', 'OL25642803M', 'OL20541993M']

def collect_single_book_data(base_url, open_library_id, file_format):
    try:
        book_details_url = base_url + open_library_id + file_format
        print(book_details_url)
        response = requests.get(book_details_url)
        return response
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return ERROR

def main():
    print("Strating...")
    for book in books_list:
        response = collect_single_book_data(OPEN_LIBRARY_BASE_URL, book, FILE_FORMAT)
        datetime_now = datetime.now().strftime("%Y%m%d")
        book_name = f"{datetime_now}_book_{book}"
        save_data(response, book_name, FILE_FORMAT)

main()