import enum
from .web_driver import get_default_web_driver
from seleniumwire.request import Request, Response
from mimetypes import guess_extension
from typing import Dict
import time
import numpy as np
from .io import DownloaderIOOps
from .common import DownloaderState, PInterestImageResolution
import traceback
import urllib.parse


_image_file_extensions = ('.jpg', '.jpeg', '.gif', '.webp', '.png')


class _ImageState(enum.Enum):
    downloaded = enum.auto()
    pending = enum.auto()
    rejected = enum.auto()
    fail = enum.auto()


_get_pinterest_image_resolution_enum = {
    'originals': PInterestImageResolution.Originals,
    '736x': PInterestImageResolution.p_736x,
    '564x': PInterestImageResolution.p_564x,
    '474x': PInterestImageResolution.p_474x,
    '236x': PInterestImageResolution.p_236x,
    '170x': PInterestImageResolution.p_170x,
    '75x75_RS': PInterestImageResolution.p_75x75_RS
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


def _parse_requests(requests, io_operator: DownloaderIOOps, task_state: Dict[str, _ImageContext], target_resolution: PInterestImageResolution):
    downloaded_images = []
    new_requests = []
    for request in requests:
        ret = _parse_request(request)
        if ret is None:
            continue
        image_file_name, image_context = ret

        if io_operator.has_file(image_file_name):
            continue

        downloaded_image = image_file_name, image_context.content, image_context.url
        if image_file_name not in task_state:
            # new image
            if image_context.resolution < target_resolution:
                new_requests.append((image_context.url, target_resolution))
                if image_context.state == _ImageState.downloaded:
                    image_context.state = _ImageState.pending
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
                    if task_state[image_file_name].state == _ImageState.pending:
                        image_context = task_state[image_file_name]
                        downloaded_image = image_file_name, image_context.content, image_context.url
                        image_context.state = _ImageState.downloaded
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


def _save_downloaded_images(downloaded_images, io_operator: DownloaderIOOps, num_downloaded_images, target_number, disp_prefix: str):
    for image_file_name, image_content, image_url in downloaded_images:
        io_operator.save(image_file_name, image_content)
        io_operator.save_meta(image_file_name, image_url)
        num_downloaded_images += 1
        if disp_prefix is not None:
            print(f'{disp_prefix}: ', end='')
        print(f'{num_downloaded_images}/{target_number} {image_file_name}')


def _download_loop(driver, io_operator: DownloaderIOOps, num_downloaded_images, target_number: int,
                   target_resolution: PInterestImageResolution, task_state: dict,
                   rng: np.random.Generator, disp_prefix: str):
    try_times = 100
    tried_times = 0
    sleep_time = 1 / 6
    last_run_downloaded = num_downloaded_images.item()

    page_height = 0

    while True:
        if tried_times > try_times:
            return num_downloaded_images - last_run_downloaded > 0

        downloaded_images, new_requests = _parse_requests(driver.requests, io_operator, task_state, target_resolution)
        del driver.requests

        if len(downloaded_images) == 0 and len(new_requests) == 0:
            tried_times += 1
        else:
            tried_times = 0

        _save_downloaded_images(downloaded_images, io_operator, num_downloaded_images, target_number, disp_prefix)
        _launch_new_requests(driver, new_requests)

        if num_downloaded_images < target_number:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(rng.random() * sleep_time * 2)
            new_page_height = driver.execute_script("return document.body.scrollHeight;")
            if new_page_height > page_height:
                page_height = new_page_height
                tried_times = 0
        else:
            return True


def download_wordnet_id_search_result_from_pinterest(wordnet_id: str, search_name: str, workspace_dir: str,
                                                     db_config: dict, target_number: int, target_resolution,
                                                     proxy_address: str, headless: bool,
                                                     file_lock_expired_time: int = 1800  # half hour
                                                     ):
    io_operator = DownloaderIOOps(wordnet_id, workspace_dir, db_config, file_lock_expired_time)
    if not io_operator.try_lock():
        return DownloaderState.Skipped, 0

    with io_operator:
        rng = np.random.Generator(np.random.PCG64())
        disp_prefix = f'{search_name}({wordnet_id})'

        num_downloaded_images = io_operator.count()

        if num_downloaded_images >= target_number:
            return DownloaderState.Done, num_downloaded_images

        num_downloaded_images = np.asarray(num_downloaded_images)
        fault_tolerance = 2
        tried_times = 0

        task_state = {}

        while True:
            if tried_times == fault_tolerance:
                break
            try:
                driver = get_default_web_driver(proxy_address, headless)
                with driver:
                    driver.get(f'https://id.pinterest.com/search/pins/?q={urllib.parse.quote(search_name)}&rs=typed')
                    success_flag = _download_loop(driver, io_operator, num_downloaded_images, target_number,
                                                  target_resolution, task_state, rng, disp_prefix)

                    rest_downloaded_images = []
                    for image_file_name, image_context in task_state.items():
                        if image_context.state == _ImageState.pending:
                            downloaded_image = image_file_name, image_context.content, image_context.url
                            rest_downloaded_images.append(downloaded_image)
                    _save_downloaded_images(rest_downloaded_images, io_operator, num_downloaded_images, target_number, disp_prefix)
                    if success_flag:
                        if num_downloaded_images < target_number:
                            return DownloaderState.Unfinished, num_downloaded_images.item()
                        else:
                            return DownloaderState.Done, num_downloaded_images.item()
                    else:
                        return DownloaderState.Fail, num_downloaded_images.item()
            except Exception as e:
                print(traceback.format_exc())
                tried_times += 1

                debug = True
                if debug:
                    raise e
