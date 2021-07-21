import enum


_image_file_extensions = ('.jpg', '.jpeg', '.gif', '.webp', '.png')


class PInterestImageResolution(enum.IntEnum):
    p_75x75_RS = enum.auto()
    p_170x = enum.auto()
    p_236x = enum.auto()
    p_474x = enum.auto()
    p_564x = enum.auto()
    p_736x = enum.auto()
    Originals = enum.auto()


class DownloaderState(enum.Enum):
    Done = enum.auto()
    Unfinished = enum.auto()
    Fail = enum.auto()
    Skipped = enum.auto()
