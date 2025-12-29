import argparse
import yaml
import time
import requests
from pymongo import MongoClient
from datetime import datetime
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

def load_config(path):
    with open(path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)

def normalize_url(url):
    return url.strip().rstrip('/')

def run_robot(config_path):
    config = load_config(config_path)

    db_settings = config['db']
    client = MongoClient(db_settings['host'], db_settings['port'])
    db = client[db_settings['database_name']]
    collection = db[db_settings['collection_name']]

    delay = config['logic']['delay']
    max_pages_limit = config['logic'].get('max_pages', 1000)

    exclude_prefixes = config['logic'].get('exclude_prefixes', [])

    def is_url_allowed(url):
        for prefix in exclude_prefixes:
            if url.startswith(prefix):
                return False
        return True

    queue = []
    visited = set()

    for target in config['targets']:
        url = normalize_url(target['url'])
        if is_url_allowed(url):
            queue.append({
                'url': url,
                'source_name': target['source_name'],
                'base_domain': urlparse(url).netloc
            })

    pages_crawled = 0

    while queue and pages_crawled < max_pages_limit:
        task = queue.pop(0)
        current_url = task['url']
        source_name = task['source_name']
        base_domain = task['base_domain']

        if current_url in visited:
            continue

        visited.add(current_url)

        try:
            print(f"[{pages_crawled + 1}/{max_pages_limit}] Downloading: {current_url}")
            response = requests.get(current_url, timeout=10)

            if response.status_code == 200:
                html_text = response.text

                timestamp = int(datetime.now().timestamp())
                document = {
                    "url": current_url,
                    "raw_html": html_text,
                    "source_name": source_name,
                    "crawled_at": timestamp
                }
                collection.insert_one(document)
                pages_crawled += 1

                soup = BeautifulSoup(html_text, 'html.parser')

                for link_tag in soup.find_all('a'):
                    href = link_tag.get('href')

                    if href:
                        full_url = urljoin(current_url, href)
                        normalized_new_url = normalize_url(full_url)

                        if (base_domain in normalized_new_url and
                                normalized_new_url not in visited and
                                is_url_allowed(normalized_new_url)):
                            queue.append({
                                'url': normalized_new_url,
                                'source_name': source_name,
                                'base_domain': base_domain
                            })
            else:
                print(f"Skipping {current_url}: Status {response.status_code}")

        except Exception as e:
            print(f"Error processing {current_url}: {e}")

        time.sleep(delay)

    print("Crawl finished!")
    print(f"Total pages saved: {pages_crawled}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('config_path', type=str)
    args = parser.parse_args()
    run_robot(args.config_path)
