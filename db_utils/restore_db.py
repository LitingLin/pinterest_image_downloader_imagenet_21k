from impl.db.DAO import PInterestCrawlerDAO
from contextlib import closing
import csv


def db_restore_records_from_csv(db_config: dict, csv_file: str):
    dao = PInterestCrawlerDAO(db_config)
    with dao:
        with closing(dao.get_cursor()) as cursor, open(csv_file, 'r', newline='') as fid:
            csv_reader = csv.reader(fid, delimiter=',')
            for row in csv_reader:
                if len(row) == 0:
                    continue
                assert len(row) == 3
                wordnet_id, file_name, url = row
                dao.insert(cursor, wordnet_id, file_name, url)


import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Restore records from CSV file to mysql server')
    parser.add_argument('csv_file', type=str, help='CSV file path')
    args = parser.parse_args()

    from db_utils._get_db_config import get_db_config
    db_restore_records_from_csv(get_db_config(), args.csv_file)
