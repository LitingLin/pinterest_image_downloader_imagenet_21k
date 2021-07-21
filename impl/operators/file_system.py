import os
import time
from ..common import _image_file_extensions


class FileSystemOperators:
    def __init__(self, folder: str):
        self.folder = folder

    def _get_lock_file(self):
        return os.path.join(self.folder, '.lock')

    def _try_create_lock_file(self):
        open_mode = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_TRUNC
        try:
            fd = os.open(self._get_lock_file(), open_mode)
            os.close(fd)
            return True
        except OSError:
            return False

    def try_lock(self, expired_time):
        if self._try_create_lock_file():
            return True
        lock_file = self._get_lock_file()
        try:
            elapsed_time = time.time() - os.path.getmtime(lock_file)
            if elapsed_time < expired_time:
                return False
            else:
                os.remove(lock_file)
        except OSError:
            return False
        return self._try_create_lock_file()

    def release_lock(self):
        lock_file = os.path.join(self.folder, '.lock')
        try:
            os.remove(lock_file)
        except OSError:
            pass

    def has_file(self, image_file_name: str):
        return os.path.exists(os.path.join(self.folder, image_file_name))

    def count(self):
        files = os.listdir(self.folder)
        files = [file for file in files if file.endswith(_image_file_extensions)]
        return len(files)

    def save(self, image_file_name: str, content: bytes):
        path = os.path.join(self.folder, image_file_name)
        with open(path + '.tmp', 'wb') as f:
            f.write(content)
        try:
            os.rename(path + '.tmp', path)
        except OSError:
            os.remove(path)
            os.rename(path + '.tmp', path)

    def save_meta(self, image_file_name: str, image_url: str):
        with open(os.path.join(self.folder, 'meta.csv'), 'a') as f:
            f.write(f"{image_file_name},{image_url}\n")
