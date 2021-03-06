import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from impl.db.DAO import PInterestCrawlerDAO
from contextlib import closing


def db_drop(db_config: dict):
    dao = PInterestCrawlerDAO(db_config)
    with dao:
        with closing(dao.get_cursor()) as cursor:
            dao.drop_table(cursor)


if __name__ == '__main__':
    char = input('Please make sure! Type yes to continue:')
    if char != 'yes':
        sys.exit()
    print('Dropping database...', end='')
    from db_utils._get_db_config import get_db_config
    db_drop(get_db_config())
    print('Done')
