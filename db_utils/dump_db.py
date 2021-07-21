from impl.db.DAO import PInterestCrawlerDAO
import os
from contextlib import closing
from datetime import datetime
import csv


def db_dump_records(db_config: dict, save_folder: str, with_id_offset: bool = True):
    id_file = os.path.join(save_folder, 'dumped_max_id.txt')
    dumped_max_id = -1
    if with_id_offset:
        if os.path.exists(id_file):
            with open(id_file) as f:
                dumped_max_id = int(f.read().strip())
    dao = PInterestCrawlerDAO(db_config)
    csv_file_name = f'{datetime.now().strftime("%Y.%m.%d-%H.%M.%S-%f")}.csv'
    id_ = None
    with dao:
        with closing(dao.get_cursor(buffered=True)) as cursor, open(csv_file_name, 'w', newline='') as fid:
            csv_writer = csv.writer(fid, delimiter=',')
            for id_, wordnet_id_and_file_name, url in dao.get_iterator_with_id_limits(cursor, dumped_max_id + 1):
                wordnet_id = wordnet_id_and_file_name[:9]
                file_name = wordnet_id_and_file_name[10:]
                csv_writer.writerow((wordnet_id, file_name, url))
    if with_id_offset and id_ is not None:
        with open(id_file, 'w') as fid:
            fid.write(str(id_))


import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Dump meta data from mysql to CSV files')
    parser.add_argument('save_folder', type=str, help='Folder path to store CSV')
    parser.add_argument('--track-dumped-id', type=bool, help='Skip dumped records')
    args = parser.parse_args()

    from db_utils._get_db_config import get_db_config
    db_dump_records(get_db_config(), args.save_folder, args.track_dumped_id)
