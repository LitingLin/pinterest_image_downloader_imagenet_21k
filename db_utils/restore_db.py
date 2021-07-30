import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from impl.db.DAO import PInterestCrawlerDAO
from contextlib import closing
import csv
from tqdm import tqdm


def db_restore_records_from_csv(db_config: dict, csv_file: str):
    buffer_size = 100
    buffered_wordnet_ids = []
    buffered_file_names = []
    buffered_urls = []
    dao = PInterestCrawlerDAO(db_config)
    with dao:
        try:
            with closing(dao.get_cursor()) as cursor, open(csv_file, 'r', newline='') as fid:
                csv_reader = csv.reader(fid, delimiter=',')
                for row in tqdm(csv_reader):
                    if len(row) == 0:
                        continue
                    assert len(row) == 3
                    wordnet_id, file_name, url = row
                    buffered_wordnet_ids.append(wordnet_id)
                    buffered_file_names.append(file_name)
                    buffered_urls.append(url)
                    if len(buffered_wordnet_ids) >= buffer_size:
                        dao.insert_multiple(cursor, buffered_wordnet_ids, buffered_file_names, buffered_urls)
                        buffered_wordnet_ids.clear()
                        buffered_file_names.clear()
                        buffered_urls.clear()
                if len(buffered_wordnet_ids) > 0:
                    dao.insert_multiple(cursor, buffered_wordnet_ids, buffered_file_names, buffered_urls)
            dao.commit()
        except Exception:
            dao.rollback()
            raise


import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Restore records from CSV file to mysql server')
    parser.add_argument('csv_file', type=str, help='CSV file path')
    args = parser.parse_args()

    from db_utils._get_db_config import get_db_config
    db_restore_records_from_csv(get_db_config(), args.csv_file)
