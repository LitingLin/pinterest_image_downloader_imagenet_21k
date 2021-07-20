import os


def has_image_file(image_file_name: str, target_folder: str):
    return os.path.exists(os.path.join(target_folder, image_file_name))
