from impl.db.DAO import PInterestCrawlerDAO
import json
from contextlib import closing


if __name__ == '__main__':
    with open('db_config.json') as f:
        db_connection_config = json.load(f)
    dao = PInterestCrawlerDAO(db_connection_config)
    with dao:
        with closing(dao.get_cursor()) as cursor:
            dao.create_table(cursor)