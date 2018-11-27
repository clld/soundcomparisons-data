"""
Gets the latest Sound-Comparisons DB backup (via its UID) from CDSTAR 
and loads it to a MariaDB specified via 
    --sc-host, --db-name, --db-user, --db-password 
and exports all tables to {repo_root}/raw/{table}.csv

>>> python ./to_rawcsv.py --sc-host 192.168.56.3 --db-password pwd

"""

import sys
import os
import requests
import argparse
import tempfile

from requests.auth import HTTPBasicAuth
from fabric.api import local
from pycdstar.api import Cdstar
from pathlib import Path
from pysoundcomparisons.db import DB
from clldutils.dsv import UnicodeWriter
from collections import OrderedDict

CDSTAR_URL = os.environ.get('CDSTAR_URL')
CDSTAR_USER = os.environ.get('CDSTAR_USER_BACKUP')
CDSTAR_PW = os.environ.get('CDSTAR_PWD_BACKUP')
DB_DUMP_UID = 'EAEA0-D042-6B44-6176-0'


def main():

    # # local dump file name
    dump_file = str(Path(tempfile.mkdtemp()) / 'db_dump.gz')

    parser = argparse.ArgumentParser('pysoundcomparisons')
    parser.add_argument('--sc-host', default='localhost')
    parser.add_argument('--db-name', default='soundcomparisons')
    parser.add_argument('--db-user', default='soundcomparisons')
    parser.add_argument('--db-password', default='pwd')
    args = parser.parse_args()

    # download lastest Sound-Comparisons database dump as gz file
    cdstar = Cdstar(user=CDSTAR_USER, password=CDSTAR_PW, service_url=CDSTAR_URL)
    search_res = cdstar.search(DB_DUMP_UID)
    if search_res.hitcount is 0:
        raise ValueError('Nothing found.')
    if len(search_res[0].resource.bitstreams) == 0:
        raise ValueError('No bitstream found.')
    latest_bs = search_res[0].resource.bitstreams[-1]
    with open(dump_file, 'wb') as f:
        f.write(requests.get("%s/bitstreams/%s/%s" % (CDSTAR_URL, DB_DUMP_UID, latest_bs.id),
                        auth=HTTPBasicAuth(CDSTAR_USER, CDSTAR_PW)).content)

    # load data into MariaDB
    db = DB(host=args.sc_host, db=args.db_name, user=args.db_user, password=args.db_password)
    db("DROP DATABASE IF EXISTS %s" % (args.db_name))
    db("CREATE DATABASE %s" % (args.db_name))
    local('gunzip -c %s | mysql -h %s -u %s -p%s -D %s' % (dump_file,
            args.sc_host, args.db_user, args.db_password, args.db_name))

    # specify CSV output path
    out_path = Path(os.getcwd()).parent.parent / 'raw'
    if not out_path.exists():
        out_path.mkdir()

    # export all base tables as CSV files
    db("USE %s" % (args.db_name))
    excludeFields = []
    excludeTables = ['renamed_soundfiles']
    for t in list(db("SHOW FULL TABLES WHERE Table_Type = 'BASE TABLE'")):
        table = t[0]
        if table in excludeTables:
            continue
        print(table)
        res = db("SELECT * FROM %s" % (table))
        header = res.keys()
        with UnicodeWriter('%s.csv' % (str(out_path / table))) as w:
            h = [c for c in header if c not in excludeFields]
            for c in h:
                print('    %s' % (c))
            w.writerow(h)
            for row in list(res):
                d = OrderedDict(zip(header, row))
                for k in excludeFields:
                    if k in d:
                        del d[k]
                w.writerow(d.values())


if __name__ == "__main__":
    main()
