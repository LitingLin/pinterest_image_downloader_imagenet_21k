import enum
from collections import namedtuple
from .web_driver import get_default_web_driver
from seleniumwire.request import Request, Response
from mimetypes import guess_extension
from typing import Dict

_image_file_extensions = ('.jpg', '.jpeg', '.gif', '.webp', '.png')


class PInterestImageResolution(enum.IntEnum):
    _75x75_RS = enum.auto()
    _170x = enum.auto()
    _236x = enum.auto()
    _474x = enum.auto()
    _564x = enum.auto()
    _736x = enum.auto()
    Originals = enum.auto()


class _ImageState(enum.Enum):
    downloaded = enum.auto()
    pending = enum.auto()
    rejected = enum.auto()
    fail = enum.auto()


_get_pinterest_image_resolution_enum = {
    'originals': PInterestImageResolution.Originals,
    '736x': PInterestImageResolution._736x,
    '564x': PInterestImageResolution._564x,
    '474x': PInterestImageResolution._474x,
    '236x': PInterestImageResolution._236x,
    '170x': PInterestImageResolution._170x,
    '75x75_RS': PInterestImageResolution._75x75_RS
}


_get_pinterest_image_resolution_from_enum = {
    v: k for k, v in _get_pinterest_image_resolution_enum.items()
}


class _ImageContext:
    state: _ImageState
    url: str
    resolution: PInterestImageResolution
    content: bytes


def _get_image_file_name_from_url(url: str):
    return url.split('/')[-1]


def _get_image_resolution_string_index_from_url(url: str):
    domain = 'i.pinimg.com'
    i_1 = url.find(domain)
    assert i_1 != -1
    i_1 += len(domain)
    assert url[i_1] == '/'
    i_1 += 1
    i_2 = url[i_1:].find('/')
    assert i_2 > 0
    return i_1, i_1 + i_2


def _get_image_resolution_from_url(url: str):
    i_1, i_2 = _get_image_resolution_string_index_from_url(url)
    return _get_pinterest_image_resolution_enum[url[i_1: i_2]]


def _get_new_url_with_desire_resolution(url: str, resolution: PInterestImageResolution):
    i_1, i_2 = _get_image_resolution_string_index_from_url(url)
    return url[: i_1] + _get_pinterest_image_resolution_from_enum[resolution] + url[i_2:]


def _is_pinterest_image_server_url(url: str):
    if 'i.pinimg.com' not in url:
        return False

    if not url.endswith(_image_file_extensions):
        return False

    return True


def _is_valid_request(request: Request):
    if request.response is None:
        return False


def _parse_request(request: Request):
    if not _is_pinterest_image_server_url(request.url):
        return None

    image_file_name = _get_image_file_name_from_url(request.url)
    image_context = _ImageContext()
    image_context.url = request.url
    image_context.state = _ImageState.rejected
    image_context.resolution = _get_image_resolution_from_url(request.url)
    image_context.content = None
    response: Response = request.response

    ret = (image_file_name, image_context)

    if response is None:
        return ret

    if response.status_code < 200 or response.status_code >= 300:
        return ret

    content_type = response.headers['Content-Type']
    if content_type is None:
        return ret

    ext = guess_extension(content_type.split(';')[0].strip())
    if ext is None or ext not in _image_file_extensions:
        return ret

    if response.body is None:
        return ret

    image_context.state = _ImageState.downloaded
    image_context.content = response.body
    return ret


def _parse_requests(requests, task_state: Dict[str, _ImageContext], target_resolution: PInterestImageResolution):
    downloaded_images = []
    new_requests = []
    for request in requests:
        ret = _parse_request(request)
        if ret is None:
            continue
        image_file_name, image_context = ret
        downloaded_image = image_file_name, image_context.content
        if image_file_name not in task_state:
            # new image
            if image_context.resolution < target_resolution:
                new_requests.append((image_context.url, target_resolution))
                task_state[image_file_name] = image_context
            else:
                if image_context.state == _ImageState.downloaded:
                    downloaded_images.append(downloaded_image)
                    task_state[image_file_name] = image_context
        else:
            # old image, higher resolution request
            if image_context.state == _ImageState.downloaded:
                downloaded_images.append(downloaded_image)
                task_state[image_file_name] = image_context
            else:
                if image_context.resolution != min(PInterestImageResolution) and image_context.resolution - 1 > task_state[image_file_name].resolution:
                    new_requests.append((image_context.url, PInterestImageResolution(image_context.resolution - 1)))
                else:
                    if task_state[image_file_name].state == _ImageState.downloaded:
                        downloaded_images.append(downloaded_image)
                    else:
                        del task_state[image_file_name]
    return downloaded_images, new_requests


def _launch_new_requests(driver, new_requests):
    if len(new_requests) == 0:
        return
    js_script = "let imgs = ["
    for url, target_resolution in new_requests:
        url = _get_new_url_with_desire_resolution(url, target_resolution)
        js_script += f"'{url}',"
    js_script += "];\n"
    js_script += "for (let i in imgs) {\n"
    js_script += "    img = new Image();\n"
    js_script += "    img.src = imgs[i];\n"
    js_script += "}"
    driver.execute_script(js_script)


class _DownloadHandler:
    def __init__(self, driver, target_resolution, downloaded_images, disp_prefix):
        if len(downloaded_images) is not None:
            self.downloaded_images_on_last_run = set(downloaded_images)
        else:
            self.downloaded_images_on_last_run = []

        self.driver = driver
        self.target_resolution = target_resolution
        self.disp_prefix = disp_prefix
        self.try_times = 100
        self.sleep_time_per_iteration = 1 / 3

    def handle(self):
        tried_times = 0
        last_num_downloaded = len(self.downloaded_images_on_last_run)

        page_height = 0

        valid_counts = []
        image_downloading_context = {}
        while True:
            if tried_times > self.try_times:
                return any(valid_counts)
            valid_count = _download_from_requests(driver.requests, save_path, downloaded_images, target_number,
                                                  disp_prefix)
            valid_counts.append(valid_count)
            del driver.requests

            if len(downloaded_images) <= last_num_downloaded:
                tried_times += 1
            else:
                last_num_downloaded = len(downloaded_images)
                tried_times = 0

            if len(downloaded_images) < target_number:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(rng.random() / 3)
                new_page_height = driver.execute_script("return document.body.scrollHeight;")
                if new_page_height > page_height:
                    page_height = new_page_height
                    tried_times = 0
            else:
                return True

        pass







def _download_loop(driver: webdriver.Chrome, save_path: str, downloaded_images: set, target_number: int,
                   rng: np.random.Generator, disp_prefix: str):
    try_times = 100
    tried_times = 0
    last_num_downloaded = len(downloaded_images)

    page_height = 0

    valid_counts = []
    while True:
        if tried_times > try_times:
            return any(valid_counts)
        valid_count = _download_from_requests(driver.requests, save_path, downloaded_images, target_number, disp_prefix)
        valid_counts.append(valid_count)
        del driver.requests

        if len(downloaded_images) <= last_num_downloaded:
            tried_times += 1
        else:
            last_num_downloaded = len(downloaded_images)
            tried_times = 0

        if len(downloaded_images) < target_number:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(rng.random() / 3)
            new_page_height = driver.execute_script("return document.body.scrollHeight;")
            if new_page_height > page_height:
                page_height = new_page_height
                tried_times = 0
        else:
            return True
