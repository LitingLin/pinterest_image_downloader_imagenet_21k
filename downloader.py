import multiprocessing
from multiprocessing.pool import ThreadPool
import threading
from functools import partial
import os
import time
import tqdm
import json
from impl.common import DownloaderState, PInterestImageResolution
from impl.pinterest import download_wordnet_id_search_result_from_pinterest

_thread_local_variables = threading.local()
_fault_tolerance = 100


def download_worker_entry(wordnet_id: str, search_name: str, workspace_dir: str, db_config: dict, target_number: int,
                          target_resolution, proxy_address: str, headless: bool,
                          shared_value: multiprocessing.Value):
    state, count = download_wordnet_id_search_result_from_pinterest(
        wordnet_id, search_name, workspace_dir, db_config, target_number, target_resolution, proxy_address, headless)
    if state == DownloaderState.Skipped:
        count = -1
    elif state == DownloaderState.Fail:
        count = 0
    shared_value.value = count


class PInterestDownloader:
    def __init__(self, workspace_dir, enable_multiprocessing=True, proxy_address=None, headless=False,
                 database_config: dict = None):
        self.workspace_dir = workspace_dir
        self.subprocess_state_value = multiprocessing.Value('i') if enable_multiprocessing else None
        self.proxy_address = proxy_address
        self.headless = headless
        self.db_config = database_config

    def download(self, wordnet_id, search_name, target_number, target_resolution):
        if self.subprocess_state_value:
            p = multiprocessing.Process(target=download_worker_entry,
                                        args=(wordnet_id, search_name, self.workspace_dir,self.db_config, target_number,
                                              target_resolution, self.proxy_address, self.headless,
                                              self.subprocess_state_value))
            p.start()
            p.join()
            if p.exitcode != 0:
                return DownloaderState(DownloaderState.Fail), 0
            downloaded_images = self.subprocess_state_value.value
            if downloaded_images >= target_number:
                return DownloaderState.Done
            elif downloaded_images > 0:
                return DownloaderState.Unfinished
            elif downloaded_images == 0:
                return DownloaderState.Fail
            else:
                return DownloaderState.Skipped
        else:
            state, _ = download_wordnet_id_search_result_from_pinterest(wordnet_id, search_name, self.workspace_dir,
                                                                        self.db_config, target_number,
                                                                        target_resolution, self.proxy_address,
                                                                        self.headless)
            return state


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
            words = lemma.split(',')
            words = [word.strip() for word in words]
            if len(words) > 0:
                wordnet_lemmas.append(words)
    return wordnet_lemmas


def _download_wordnet_lemma_on_pinterest(downloader, target_number, target_resolution, process_bar, wordnet_id, wordnet_lemmas):
    for wordnet_lemma in wordnet_lemmas:
        process_bar.set_description(f'Downloading: {wordnet_lemma}({wordnet_id})')
        downloader_state = downloader.download(wordnet_id, wordnet_lemma, target_number, target_resolution)
        if downloader_state != DownloaderState.Fail:
            _thread_local_variables.fail_times = 0
        else:
            if hasattr(_thread_local_variables, 'fail_times'):
                _thread_local_variables.fail_times += 1
            else:
                _thread_local_variables.fail_times = 0
            if _thread_local_variables.fail_times >= _fault_tolerance:
                time.sleep(200)
                _thread_local_variables.fail_times = _fault_tolerance / 2
        process_bar.update()
        return downloader_state


_get_pinterest_image_resolution_enum = {
    'orig': PInterestImageResolution.Originals,
    '736x': PInterestImageResolution.p_736x,
    '564x': PInterestImageResolution.p_564x,
    '474x': PInterestImageResolution.p_474x,
    '236x': PInterestImageResolution.p_236x,
    '170x': PInterestImageResolution.p_170x,
    '75x75_RS': PInterestImageResolution.p_75x75_RS
}


def download(workspace_dir, desire_num_per_category: int, desire_resolution: str,
             enable_mysql: bool, enable_multiprocessing: bool = True, proxy_address: str = None, headless: bool = False,
             num_threads: int = 0, slice_begin: int = None, slice_end: int = None):
    wordnet_ids = load_wordnet_ids(os.path.join(os.path.dirname(__file__), 'imagenet21k_wordnet_ids.txt'))
    wordnet_lemmas = load_wordnet_lemmas(os.path.join(os.path.dirname(__file__), 'imagenet21k_wordnet_lemmas.txt'))
    assert len(wordnet_ids) == len(wordnet_lemmas)

    if slice_begin is not None or slice_end is not None:
        wordnet_ids = wordnet_ids[slice_begin: slice_end]
        wordnet_lemmas = wordnet_lemmas[slice_begin: slice_end]

    desire_resolution = _get_pinterest_image_resolution_enum[desire_resolution]
    database_config = None
    if enable_mysql:
        json_file_path = os.path.join(os.path.dirname(__file__), 'db_config.json')
        with open(json_file_path) as f:
            database_config = json.load(f)

    downloader = PInterestDownloader(workspace_dir, enable_multiprocessing, proxy_address, headless, database_config)

    while True:
        with tqdm.tqdm(total=len(wordnet_ids), ) as process_bar:
            download_func = partial(_download_wordnet_lemma_on_pinterest, downloader, desire_num_per_category,
                                    desire_resolution, process_bar)
            if num_threads == 0:
                states = [download_func(wordnet_id, wordnet_lemma)
                          for wordnet_id, wordnet_lemma in zip(wordnet_ids, wordnet_lemmas)]
            else:
                with ThreadPool(num_threads) as pool:
                    states = pool.starmap(download_func, zip(wordnet_ids, wordnet_lemmas))
        if all([state == DownloaderState.Done or state == DownloaderState.Skipped for state in states]):
            break


import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('workspace_dir', type=str, help='Path to store images')
    parser.add_argument('number_per_category', type=int, help='Number of images per category')
    parser.add_argument('--slice-begin', type=int, help='Begin index of categories')
    parser.add_argument('--slice-end', type=int, help='End index of categories')
    parser.add_argument('--resolution', type=str, default='736x', choices=['orig', '736x', '564x', '474x', '236x',
                                                                           '170x', '75x75_RS'],
                        help='Select image resolution preference')
    parser.add_argument('--num-threads', type=int, default=0, help='Number of concurrent threads')
    parser.add_argument('--disable-multiprocessing', action='store_true', help='Disable multiprocessing')
    parser.add_argument('--proxy', type=str, help='Proxy address')
    parser.add_argument('--headless', action='store_true', help='Running chrome in headless mode')
    parser.add_argument('--use-mysql', action='store_true', help='Using MySQL to store meta data')
    args = parser.parse_args()
    download(args.workspace_dir, args.number_per_category, args.resolution, args.use_mysql,
             not args.disable_multiprocessing, args.proxy, args.headless, args.num_threads,
             args.slice_begin, args.slice_end)
