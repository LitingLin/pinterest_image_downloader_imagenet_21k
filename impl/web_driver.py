import os

using_undetected_chrome_driver = False
if using_undetected_chrome_driver:
    try:
        import seleniumwire.undetected_chromedriver.v2 as webdriver
    except ImportError:
        from seleniumwire import webdriver
else:
    from seleniumwire import webdriver


def get_default_web_driver(proxy_address: str = None, headless: bool = False):
    seleniumwire_options = {'verify_ssl': True}
    webdriver_options = {'seleniumwire_options': seleniumwire_options}

    if headless:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        webdriver_options['options'] = chrome_options

    if proxy_address is not None:
        seleniumwire_options['proxy'] = {
            'http': proxy_address,
            'https': proxy_address,
            'no_proxy': 'localhost,127.0.0.1,[::1]'  # excludes
        }
    return webdriver.Chrome(os.path.join(os.path.dirname(__file__), '..', 'drivers/chromedriver'), **webdriver_options)
