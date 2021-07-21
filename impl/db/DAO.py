import mysql.connector


_create_table_sql_statement = '''
CREATE TABLE `Records` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `wordnet_id_and_file_name` VARCHAR(768) NOT NULL,
    `url` VARCHAR(768) NOT NULL,
    `storage_engine` SMALLINT NOT NULL,
    `create_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `modify_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_file_name` (`wordnet_id_and_file_name`)
)
'''
_drop_table_sql_statement = 'DROP TABLE `Records`'
_exists_file_sql_statement = 'SELECT EXISTS(SELECT * FROM `Records` WHERE `wordnet_id_and_file_name` = %s)'
_new_record_sql_statement = 'INSERT INTO `Records` (`wordnet_id_and_file_name`, `url`, `storage_engine`) values (%s, %s, %s)'
_count_all_sql_statement = 'SELECT COUNT(*) FROM `Records`'
_count_by_wordnet_id_sql_statement = "SELECT COUNT(*) FROM `Records` WHERE `wordnet_id_and_file_name` LIKE %s"
_select_all_sql_statement = 'SELECT * from `Records`'
_select_id_file_url_sql_statement = 'SELECT `id`, `wordnet_id_and_file_name`, `url` from `Records`'


def _concatenate_wordnet_id_file_name(wordnet_id, file_name):
    assert len(wordnet_id) == 9
    return wordnet_id + '-' + file_name


class PInterestCrawlerDAO:
    def __init__(self, connection_config: dict):
        self.connection_config = connection_config

    def __enter__(self):
        self.ctx = mysql.connector.connect(**self.connection_config)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.ctx.close()

    def get_cursor(self, buffered=False):
        return self.ctx.cursor(buffered=buffered)

    def exists(self, cursor, wordnet_id: str, file_name: str):
        cursor.execute(_exists_file_sql_statement, (_concatenate_wordnet_id_file_name(wordnet_id, file_name), ))
        result = cursor.fetchone()[0]
        return result == 1

    def insert(self, cursor, wordnet_id: str, file_name: str, url: str, storage_engine: int = 0):
        try:
            cursor.execute(_new_record_sql_statement, (_concatenate_wordnet_id_file_name(wordnet_id, file_name), url,
                                                       storage_engine))
            self.ctx.commit()
            return True, None, None
        except mysql.connector.Error as e:
            self.ctx.rollback()
            return False, e.errno, str(e)

    def create_table(self, cursor):
        cursor.execute(_create_table_sql_statement)

    def drop_table(self, cursor):
        cursor.execute(_drop_table_sql_statement)

    def count_all(self, cursor):
        cursor.execute(_count_all_sql_statement)
        return cursor.fetchone()[0]

    def count_by_wordnet_id(self, cursor, wordnet_id):
        cursor.execute(_count_by_wordnet_id_sql_statement, (wordnet_id + '%',))
        return cursor.fetchone()[0]

    def get_iterator(self, cursor):
        cursor.execute(_select_id_file_url_sql_statement)
        return cursor

    def get_iterator_with_id_limits(self, cursor, id_min: int=None, id_max: int=None):
        if id_min is None and id_max is None:
            cursor.execute(_select_id_file_url_sql_statement)
        elif id_min is not None and id_max is not None:
            cursor.execute(_select_id_file_url_sql_statement + ' WHERE `id` >= %s AND `id` <= %s', (id_min, id_max))
        elif id_min is not None:
            cursor.execute(_select_id_file_url_sql_statement + ' WHERE `id` >= %s', (id_min,))
        elif id_max is not None:
            cursor.execute(_select_id_file_url_sql_statement + ' WHERE `id` <= %s', (id_max,))
        else:
            raise Exception
        return cursor
