from .web_driver import get_default_web_driver
from seleniumwire.request import Request, Response
import numpy as np
import time


def _parse_request(request: Request):
    response: Response = request.response
    if response is None:
        return request.url

    # return request.url, response.status_code, response.headers, response.headers['Content-Type'], response.reason, response.body
    return response.headers['Content-Type']


def _run_url(url: str, iterations: int, wait_time_per_iteration: float, enable_randomness: bool = True):
    if enable_randomness:
        rng = np.random.Generator(np.random.PCG64())
    web_driver = get_default_web_driver()
    with web_driver:
        web_driver.get(url)
        for _ in range(iterations):
            for request in web_driver.requests:
                print(_parse_request(request))
            del web_driver.requests
            wait_time = wait_time_per_iteration
            if enable_randomness:
                wait_time = rng.random() * 2 * wait_time
            time.sleep(wait_time)
