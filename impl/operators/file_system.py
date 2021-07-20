import os
from ..common import _image_file_extensions


class FileSystemOperators:
    def __init__(self, folder: str):
        self.folder = folder

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
