from impl.db.DAO import PInterestCrawlerDAO
import json
from contextlib import closing


if __name__ == '__main__':
    with open('db_config.json') as f:
        db_connection_config = json.load(f)
    dao = PInterestCrawlerDAO(db_connection_config)
    with dao:
        # dao.create_table()
        # dao.insert('n111', 'sdfewfe', 0)
        with closing(dao.get_cursor()) as cursor:
            print(type(dao.count_all(cursor)))
            print(dao.count_by_wordnet_id(cursor, 'n111'))
            print(dao.insert(cursor, 'n111', 'sdfewfe', 0))
            print(dao.exists(cursor, 'n111', 'sdfewfe'))
            for data in dao.get_iterator_with_id_limits(cursor, 2):
                print(data)
