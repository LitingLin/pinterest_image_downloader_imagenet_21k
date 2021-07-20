import multiprocessing
from multiprocessing.pool import ThreadPool
import threading
from functools import partial
from mimetypes import guess_extension
import os
import urllib.parse
import time
import enum
import tqdm
import numpy as np
import traceback
using_undetected_chrome_driver = True
from collections import namedtuple
if using_undetected_chrome_driver:
    try:
        import seleniumwire.undetected_chromedriver.v2 as webdriver
    except ImportError:
        from seleniumwire import webdriver
else:
    from seleniumwire import webdriver


class DownloaderState(enum.Enum):
    Ok = enum.auto()
    Partly = enum.auto()
    Fail = enum.auto()


_image_file_extensions = ('.jpg', '.jpeg', '.gif', '.webp', '.png')
_thread_local_variables = threading.local()
_fault_tolerance = 100

_pinterest_image_resolution_list = ['originals', '736x', '564x', '474x', '236x', '170x', '75x75_RS']


class PInterestImageResolution(enum.IntEnum):
    _75x75_RS = enum.auto()
    _170x = enum.auto()
    _236x = enum.auto()
    _474x = enum.auto()
    _564x = enum.auto()
    _736x = enum.auto()
    Originals = enum.auto()


_get_pinterest_image_resolution_enum = {
    'originals': PInterestImageResolution.Originals,
    '736x': PInterestImageResolution._736x,
    '564x': PInterestImageResolution._564x,
    '474x': PInterestImageResolution._474x,
    '236x': PInterestImageResolution._236x,
    '170x': PInterestImageResolution._170x,
    '75x75_RS': PInterestImageResolution._75x75_RS
}


ImageDownloadState = namedtuple('ImageDownloadState', ('url', 'resolution'))

class _DownloadHandler:
    def __init__(self, driver: webdriver.Chrome, target_resolution):
        assert target_resolution in _pinterest_image_resolution_list

        self.driver = driver
        self.target_resolution = target_resolution

    def _parse_downloaded_images(self, downloaded_images):
        pass

    def handle(self):
        try_times = 100
        tried_times = 0
        last_num_downloaded = len(downloaded_images)

        page_height = 0

        valid_counts = []
        while True:
            if tried_times > try_times:
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






def _download_from_requests(requests, save_path, downloaded_images: set, target_number, disp_prefix: str):
    valid_count = 0
    for request in requests:
        if 'i.pinimg.com' not in request.url:
            continue
        if request.response is None:
            continue
        content_type = request.response.headers['Content-Type']
        if content_type is None:
            continue
        ext = guess_extension(content_type.split(';')[0].strip())
        if ext is None or ext not in _image_file_extensions:
            continue

        if request.response.body is None:
            continue
        valid_count += 1
        file_name = request.url.split('/')[-1]
        if file_name in downloaded_images:
            continue
        image_file_path = os.path.join(save_path, file_name)
        with open(image_file_path + '.tmp', 'wb') as f:
            f.write(request.response.body)
        os.rename(image_file_path + '.tmp', image_file_path)
        if disp_prefix is not None:
            print(f'{disp_prefix}: ', end='')
        print(f'{len(downloaded_images)}/{target_number} {file_name}')
        downloaded_images.add(file_name)
        if len(downloaded_images) >= target_number:
            return valid_count
    return valid_count


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


def _download(search_name: str, save_path: str, target_number: int, proxy_address: str, headless: bool, disp_prefix: str):
    rng = np.random.Generator(np.random.PCG64())
    if not os.path.exists(save_path):
        os.mkdir(save_path)
        downloaded_images = set()
    else:
        images = os.listdir(save_path)
        images = [image for image in images if image.endswith(_image_file_extensions)]
        downloaded_images = set(images)

    if len(downloaded_images) >= target_number:
        return DownloaderState.Ok

    webdriver_options = {}

    if headless:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        webdriver_options['options'] = chrome_options

    if proxy_address is not None:
        webdriver_options['seleniumwire_options'] = {
            'proxy': {
                'http': proxy_address,
                'https': proxy_address,
                'no_proxy': 'localhost,127.0.0.1'  # excludes
            }
        }
    fault_tolerance = 2
    tried_times = 0
    while True:
        if tried_times == fault_tolerance:
            break
        try:
            driver = webdriver.Chrome(os.path.join(os.path.dirname(__file__), 'drivers/chromedriver'), **webdriver_options)
            with driver:
                driver.get(f'https://id.pinterest.com/search/pins/?q={urllib.parse.quote(search_name)}&rs=typed')
                success_flag = _download_loop(driver, save_path, downloaded_images, target_number, rng, disp_prefix)
                break
        except Exception:
            print(traceback.format_exc())
            tried_times += 1

    if success_flag:
        if len(downloaded_images) < target_number:
            return DownloaderState.Partly
        else:
            return DownloaderState.Ok
    else:
        return DownloaderState.Fail


def download_worker_entry(search_name, save_path, target_number, proxy_address, headless, disp_prefix: str,
                          shared_value: multiprocessing.Value):
    state = _download(search_name, save_path, target_number, proxy_address, headless, disp_prefix)
    shared_value.value = state.value


class PInterestDownloader:
    def __init__(self, enable_multiprocessing=True, proxy_address=None, headless=False):
        self.subprocess_state_value = multiprocessing.Value('i') if enable_multiprocessing else None
        self.proxy_address = proxy_address
        self.headless = headless

    def download(self, search_name, save_path, target_number, disp_prefix=None):
        if self.subprocess_state_value:
            p = multiprocessing.Process(target=download_worker_entry,
                                        args=(search_name, save_path, target_number, self.proxy_address, self.headless,
                                              disp_prefix,
                                              self.subprocess_state_value))
            p.start()
            p.join()
            if p.exitcode != 0:
                return DownloaderState(DownloaderState.Fail)
            return DownloaderState(self.subprocess_state_value.value)
        else:
            return _download(search_name, save_path, target_number, self.proxy_address, self.headless, disp_prefix)


def load_wordnet_ids(file_path: str):
    wordnet_ids = []
    with open(file_path, 'r') as f:
        while True:
            id_ = f.readline()
            if len(id_) == 0:
                break
            id_ = id_.strip()
            if len(id_) > 0:
                wordnet_ids.append(id_)
    return wordnet_ids


def load_wordnet_lemmas(file_path: str):
    wordnet_lemmas = []
    with open(file_path, 'r') as f:
        while True:
            lemma = f.readline()
            if len(lemma) == 0:
                break
            lemma = lemma.strip()
            if len(lemma) == 0:
                continue
            word = lemma.split(',')[0].strip()
            if len(word) > 0:
                wordnet_lemmas.append(word)
    return wordnet_lemmas


def _download_wordnet_lemma_on_pinterest(downloader, target_path, target_number, process_bar, wordnet_id, wordnet_lemma):
    disp_prefix = f'{wordnet_lemma}({wordnet_id})'
    process_bar.set_description(f'Downloading: {disp_prefix}')
    downloader_state = downloader.download(wordnet_lemma,
                                           os.path.join(target_path, wordnet_id), target_number, disp_prefix)
    if downloader_state != DownloaderState.Fail:
        _thread_local_variables.fail_times = 0
    else:
        _thread_local_variables.fail_times += 1
        if _thread_local_variables.fail_times >= _fault_tolerance:
            time.sleep(200)
            _thread_local_variables.fail_times = _fault_tolerance / 2
    process_bar.update()


def download(target_number, target_path, enable_multiprocessing, proxy_address, headless, num_threads=0):
    wordnet_ids = load_wordnet_ids(os.path.join(os.path.dirname(__file__), 'imagenet21k_wordnet_ids.txt'))
    wordnet_lemmas = load_wordnet_lemmas(os.path.join(os.path.dirname(__file__), 'imagenet21k_wordnet_lemmas.txt'))
    assert len(wordnet_ids) == len(wordnet_lemmas)
    downloader = PInterestDownloader(enable_multiprocessing,
                                     proxy_address,
                                     headless)

    _thread_local_variables.fail_times = 0

    with tqdm.tqdm(total=len(wordnet_ids), ) as process_bar:
        download_func = partial(_download_wordnet_lemma_on_pinterest, downloader, target_path,
                                target_number, process_bar)
        if num_threads == 0:
            for wordnet_id, wordnet_lemma in zip(wordnet_ids, wordnet_lemmas):
                download_func(wordnet_ids, wordnet_lemma)
        else:
            with ThreadPool(num_threads) as pool:
                pool.starmap(download_func, zip(wordnet_ids, wordnet_lemmas))


import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('target_number', type=int, help='Number of images per category')
    parser.add_argument('target_path', type=str, help='Path to store images')
    parser.add_argument('--resolution', type=str, default='736x', help='image resolution, availables: (orig 736x 564x 474x 236x 170x)')
    parser.add_argument('--num-threads', type=int, default=0, help='Number of concurrent threads')
    parser.add_argument('--disable-multiprocessing', action='store_true', help='Disable multiprocessing')
    parser.add_argument('--proxy', type=str, help='Proxy address')
    parser.add_argument('--headless', action='store_true', help='Running chrome in headless mode')
    args = parser.parse_args()
    download(args.target_number, args.target_path, not args.disable_multiprocessing, args.proxy, args.headless, args.num_threads)
