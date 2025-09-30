
import argparse
import os
import os.path
import time
import traceback
from datetime import datetime as dt

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options


def arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--browser', default="firefox")
    return parser.parse_args()


def download(url):

    if args.browser == 'chrome':

        opts = Options()
        opts.add_argument("--headless")

        br = webdriver.Chrome()

    elif args.browser == 'firefox':

        agent = "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6)" \
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36"

        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')
        options.set_preference("browser.download.folderList", 2)
        options.set_preference("browser.download.dir", "/home/ray/Downloads")
        options.add_argument("--disable-extensions")
        options.add_argument('--disable-application-cache')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument(agent)

        br = webdriver.Firefox(options=options)

    br.implicitly_wait(10)

    br.set_window_position(50, 50)
    br.set_window_size(900, 900)

    try:

        br.get(url)

        print("connected...")

        time.sleep(3)

        button = None

        for elt in br.find_elements(By.TAG_NAME, 'div'):
            if elt.text == 'Export':
                button = elt

        print("button found...")

        if button is not None:
            button.click()
            print("clicked!")
            time.sleep(3)

        for elt in br.find_elements(By.TAG_NAME, 'forge-button'):
            if elt.text == 'Download':
                elt.click()

        time.sleep(5)

    except:
        print("ERROR!")
    finally:
        br.quit()


def move_dload_file(dnow):

    dnow = dt.now().strftime('%Y%m%d')
    print(f"dnow: {dnow}")

    was = f"/home/ray/Downloads/Medical_Examiner-Coroner,_Full_dataset_{dnow}.csv"
    tobe = f"/home/ray/scc_coroner/coroner_data_{dnow}.csv"

    if os.path.isfile(was):
        os.rename(was, tobe)
        print(f"data: {tobe}")
        return tobe
    else:
        raise Exception(f"ERROR could not find file: {was}")


if __name__ == '__main__':

    args = arguments()

    url = 'https://data.sccgov.org/Health/Medical-Examiner-Coroner-Full-dataset/s3fb-yrjp/about_data'

    download(url)

    file = move_dload_file(dnow)

    sync_data(file)

    print("Done.")

