
import csv
import traceback
from pprint import pprint

from dotenv import dotenv_values
from sqlalchemy import create_engine, inspect

cfg = dotenv_values(".env")

db = create_engine(f"mysql+pymysql://{cfg['USR']}:{cfg['PWD']}@{cfg['HOST']}/{cfg['DB']}")
db.connect()

def db_exec(engine, sql):
    # print(f"sql: {sql}")
    if sql.strip().startswith("select"):
        return [dict(r) for r in engine.execute(sql).fetchall()]
    else:
        return engine.execute(sql)


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
            d[key] = f"'{d[key].replace("'", "''").replace('%', '%%')}'"
        else:
            if d[key] == '':
                d[key] = 'NULL'

    keys = list(d.keys())
    vals = list(d.values())
    sql = f"insert into deaths ({','.join(keys)}) values ({','.join(vals)})"
    return sql


if __name__ == '__main__':

    file = 'Medical_Examiner-Coroner,_Full_dataset_20250926.csv'

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

    print("done.")

