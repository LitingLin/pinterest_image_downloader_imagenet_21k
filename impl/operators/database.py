class DatabaseOperators:
    def __init__(self, dao, wordnet_id):
        self.dao = dao
        self.wordnet_id = wordnet_id

    def __enter__(self):
        self.cursor = self.dao.get_cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()

    def has_file(self, image_file_name: str):
        return self.dao.exists(self.cursor, self.wordnet_id, image_file_name)

    def count(self):
        return self.dao.count_by_wordnet_id(self.cursor, self.wordnet_id)

    def save_meta(self, image_file_name: str, url: str):
        ok, errno, err_msg = self.dao.insert(self.cursor, self.wordnet_id, image_file_name, url, 0)
        if ok:
            return True
        else:
            if errno == 1062:
                return False
            else:
                raise RuntimeError(err_msg)
