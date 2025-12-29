
import time
from pymongo import MongoClient
from bs4 import BeautifulSoup
from datetime import datetime

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
DB_NAME = 'russian_corpus'
RAW_COLLECTION_NAME = 'pages'
PARSED_COLLECTION_NAME = 'parsed_pages'


def format_size(bytes_size):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024


def clean_and_store():
    try:
        client = MongoClient(MONGO_HOST, MONGO_PORT)
        db = client[DB_NAME]
        raw_collection = db[RAW_COLLECTION_NAME]
        parsed_collection = db[PARSED_COLLECTION_NAME]

        parsed_collection.delete_many({})

        cursor = raw_collection.find({})
        total_docs_in_db = raw_collection.count_documents({})

        if total_docs_in_db == 0:
            print("ERROR: No documents found.")
            return

        processed_count = 0
        total_raw_size = 0
        total_parsed_size = 0

        print(f"Found {total_docs_in_db} documents to process.\n")

        for doc in cursor:
            url = doc.get('url', 'N/A')
            raw_html = doc.get('raw_html')

            if not raw_html:
                continue

            raw_size = len(raw_html.encode('utf-8'))
            total_raw_size += raw_size

            soup = BeautifulSoup(raw_html, 'html.parser')
            clean_text = soup.get_text(separator=' ', strip=True)

            parsed_size = len(clean_text.encode('utf-8'))
            total_parsed_size += parsed_size

            document = {
                "url": url,
                "clean_text": clean_text,
                "processed_at": int(datetime.now().timestamp())
            }
            parsed_collection.insert_one(document)

            processed_count += 1
            if processed_count % 10 == 0:
                print(f"Processed {processed_count}/{total_docs_in_db}...")

        if processed_count > 0:
            avg_raw_size = total_raw_size / processed_count
            avg_parsed_size = total_parsed_size / processed_count
        else:
            avg_raw_size = avg_parsed_size = 0

        print("\n" + "=" * 40)
        print("Статистика для отчета")
        print("=" * 40)
        print(f"Всего документов обработано: {processed_count}")
        print(f"Общий объем 'сырых' данных: {format_size(total_raw_size)}")
        print(f"Общий объем чистого текста: {format_size(total_parsed_size)}")
        print(f"Средний размер сырого документа: {format_size(avg_raw_size)}")
        print(f"Средний объем текста в документе: {format_size(avg_parsed_size)}")
        print("=" * 40)

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    clean_and_store()
