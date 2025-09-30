
import argparse
import csv
import os
import os.path
import time
import traceback
from datetime import datetime as dt
from pprint import pprint

from dotenv import dotenv_values
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from sqlalchemy import create_engine, inspect


cfg = dotenv_values(".env")

db = create_engine(f"mysql+pymysql://{cfg['USR']}:{cfg['PWD']}@{cfg['HOST']}/{cfg['DB']}")
db.connect()

def arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--browser', default="firefox", help="Either chrome or firefox, with firefox as default.")
    parser.add_argument('--in-file', help="If you have already scraped.")
    parser.add_argument('--dry-run', action="store_true", help="Get data and compare, but do not save in db.")
    return parser.parse_args()


def db_exec(engine, sql):
    # print(f"sql: {sql}")
    if sql.strip().startswith("select"):
        return [dict(r) for r in engine.execute(sql).fetchall()]
    else:
        return engine.execute(sql)


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


def get_max_pk(conn, table_name):
    pk_rows = db_exec(conn, f"select max(pk) as max from {table_name}")
    if len(pk_rows) == 0 or pk_rows[0]['max'] is None:
        return 0
    else:
        return int(pk_rows[0]['max'])


def read_new(file):

    keys = None

    newly = list()

    with open(file, newline='') as csvfile:
        rdr = csv.DictReader(csvfile)
        for row in rdr:
            # print(f"row: {row}")

            # row: {'Case Number': '25-03138', 'Case Status': 'Open', 'Manner of Death': 'N/A', 'Age': '71',
            #       'Race': 'Asian', 'Gender': 'Male', 'Death Date': '2025-09-25 20:18:00', 'Death City': 'N/A',
            #       'Death Zip': 'N/A', 'Resident City': 'San Jose', 'Resident Zip': '95121',
            #       'Incident Location': 'N/A', 'Incident City': 'N/A', 'Incident Zip': 'N/A', 'Cause of Death': '',
            #       'Other Significant Condition': 'N/A', 'Latitude': '', 'Longitude': ''}

            if not keys:
                keys = [r.lower() for r in list(row.keys())]
                keys = [r.lower().replace(' ', '_') for r in keys]
                keys = [r.lower().replace('number', 'num') for r in keys]
                keys = [r.lower().replace('other_significant_condition', 'other_condition') for r in keys]
                # print(f"keys: {keys}")

            d = dict(zip(keys, list(row.values())))

            if d['latitude']:
                d['latitude'] = d['latitude'].replace('+', '')
                while len(d['latitude']) < 11:
                    d['latitude'] = f"{d['latitude']}0"

            if d['longitude']:
                while len(d['longitude']) < 13:
                    d['longitude'] = f"{d['longitude']}0"

            newly.append(d)

    return newly


def stored_in_db():
    rows = db_exec(db, "select * from deaths where inactive is NULL and missing is NULL")
    for row in rows:
        if row['age']:
            row['age'] = str(row['age'])
        if row['latitude']:
            row['latitude'] = str(row['latitude'])
        if row['longitude']:
            row['longitude'] = str(row['longitude'])
    return rows


def deaths_data_same(older, newer):

    extras = {'pk', 'inactive', 'missing'}

    # keys should be same except for pk.
    if set(older.keys()) - set(newer.keys()) != extras:
        #print("KEYS:")
        #print(f"What? {set(older.keys()) - set(newer.keys())}")
        #quit()
        return False

    for key in list(older.keys()):
        if key not in extras and older[key] != newer[key]:
            #print(f"key differ: {key}")
            #print(f"older: {older}")
            #print(f"newer: {newer}")
            #quit()
            return False

    return True


def insert_sql(d):

    nums = ['pk', 'age', 'latitude', 'longitude']

    for key in d:
        if key not in nums:
            val = d[key].replace("'", "''").replace('%', '%%')
            d[key] = f"'{val}'"
        else:
            if d[key] == '':
                d[key] = 'NULL'

    keys = list(d.keys())
    vals = list(d.values())
    sql = f"insert into deaths ({','.join(keys)}) values ({','.join(vals)})"
    return sql


if __name__ == '__main__':

    args = arguments()

    if not args.in_file:

        url = 'https://data.sccgov.org/Health/Medical-Examiner-Coroner-Full-dataset/s3fb-yrjp/about_data'

        download(url)

        file = move_dload_file(dnow)

    else:
        file = args.in_file

    # TODO put this in methods also...

    newly = read_new(file)
    print(f"newly # {len(newly)}")

    newly = dict(zip([r['case_num'] for r in newly], newly))

    oldly = stored_in_db()
    print(f"oldly # {len(oldly)}")

    oldly = dict(zip([r['case_num'] for r in oldly], oldly))

    changed = list()
    missing = list()

    for case_num in oldly:
        if case_num not in newly:
            missing.append(case_num)

        elif not deaths_data_same(oldly[case_num], newly[case_num]):
            changed.append(case_num)

    print(f"changed # {len(changed)}")
    print(f"missing # {len(missing)}")

    if args.dry_run:
        quit()

    #for cn in changed[:5]:
    #    print(f"oldly: {oldly[cn]}")
    #    print("")
    #    print(f"newly: {newly[cn]}")
    #    print("=========================")

    for cn in missing:
        pk = oldly[cn]['pk']

        sql = f"update deaths set missing = unix_timestamp() where pk = {pk}"
        db_exec(db, sql)

    pk = get_max_pk(db, 'deaths')

    print("updating changed...")

    for cn in changed:
        old_pk = oldly[cn]['pk']

        try:

            db_exec(db, "start transaction")

            sql = f"update deaths set inactive = unix_timestamp() where pk = {old_pk}"
            # print(f"sql: {sql}")
            db_exec(db, sql)

            pk += 1
            newly[cn]['pk'] = str(pk)
            sql = insert_sql(newly[cn])
            # print(f"sql: {sql}")
            db_exec(db, sql)

            db_exec(db, "commit")
        except:
            db_exec(db, "rollback")
            traceback.print_exc()
            quit()

    print("adding newly found...")

    added = list(set(newly.keys()) - set(oldly.keys()))

    for cn in added:

        pk += 1
        newly[cn]['pk'] = str(pk)
        sql = insert_sql(newly[cn])

        try:
            db_exec(db, sql)
        except:
            traceback.print_exc()
            print("=======================")
            print(f"BAD SQL: {sql}")
            quit()

    print("Done.")

