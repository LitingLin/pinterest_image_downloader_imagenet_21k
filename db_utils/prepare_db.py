from impl.db.DAO import PInterestCrawlerDAO
from contextlib import closing


def db_prepare(db_config: dict):
    dao = PInterestCrawlerDAO(db_config)
    with dao:
        with closing(dao.get_cursor()) as cursor:
            dao.create_table(cursor)


if __name__ == '__main__':
    from db_utils._get_db_config import get_db_config
    db_prepare(get_db_config())
