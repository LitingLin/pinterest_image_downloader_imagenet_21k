import os
try:
    from .operators.database import DatabaseOperators
    from .db.DAO import PInterestCrawlerDAO
    _db_available = True
except ImportError:
    _db_available = False
from .operators.file_system import FileSystemOperators


class DownloaderIOOps:
    def __init__(self, wordnet_id: str, workspace_dir: str, db_config: dict, file_lock_expired_time: int):
        folder = os.path.join(workspace_dir, wordnet_id)
        if not os.path.exists(folder):
            os.mkdir(folder)
        self.folder = folder
        self.wordnet_id = wordnet_id
        if not _db_available and db_config is not None:
            raise RuntimeError('Install mysql-connector-python')
        self.db_dao = None
        if db_config is not None:
            self.db_dao = PInterestCrawlerDAO(db_config)
            self.db_ops = DatabaseOperators(self.db_dao, self.wordnet_id)
        self.fs_ops = FileSystemOperators(folder)
        self.file_lock_expired_time = file_lock_expired_time
        self.locked = False

    def try_lock(self):
        self.locked = self.fs_ops.try_lock(self.file_lock_expired_time)
        return self.locked

    def release_lock(self):
        if self.locked:
            self.fs_ops.release_lock()
            self.locked = False

    def __enter__(self):
        if not self.locked:
            assert self.fs_ops.try_lock(self.file_lock_expired_time)
        if self.db_dao is not None:
            self.db_dao.__enter__()
            self.db_ops.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db_dao is not None:
            self.db_ops.__exit__(exc_type, exc_val, exc_tb)
            self.db_dao.__exit__(exc_type, exc_val, exc_tb)
        self.fs_ops.release_lock()

    def has_file(self, image_file_name: str):
        if self.db_dao is not None:
            return self.db_ops.has_file(image_file_name)
        else:
            return self.fs_ops.has_file(image_file_name)

    def count(self):
        if self.db_dao is not None:
            return self.db_ops.count()
        else:
            return self.fs_ops.count()

    def save(self, image_file_name: str, content: bytes):
        self.fs_ops.save(image_file_name, content)

    def save_meta(self, image_file_name: str, image_url: str):
        if self.db_dao is not None:
            self.db_ops.save_meta(image_file_name, image_url)
        else:
            self.fs_ops.save_meta(image_file_name, image_url)
