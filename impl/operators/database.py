from impl.perf_stat.function_call import record_running_time


class DatabaseOperators:
    def __init__(self, dao, wordnet_id):
        self.dao = dao
        self.wordnet_id = wordnet_id

    @record_running_time
    def __enter__(self):
        self.cursor = self.dao.get_cursor()

    @record_running_time
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()

    @record_running_time
    def has_file(self, image_file_name: str):
        return self.dao.exists(self.cursor, self.wordnet_id, image_file_name)

    @record_running_time
    def count(self):
        return self.dao.count_by_wordnet_id(self.cursor, self.wordnet_id)

    @record_running_time
    def save_meta(self, image_file_name: str, url: str):
        ok, errno, err_msg = self.dao.insert_and_commit(self.cursor, self.wordnet_id, image_file_name, url)
        if ok:
            return True
        else:
            if errno == 1062:
                return False
            else:
                raise RuntimeError(err_msg)
