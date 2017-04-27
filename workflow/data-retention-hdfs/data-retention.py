#!/usr/bin/env python

from subprocess import Popen, PIPE
import time
import traceback
import argparse

from datetime import datetime, timedelta, date

parser = argparse.ArgumentParser()
parser.add_argument("--sourcedir", help="Source directory")
parser.add_argument("--retention", help="Days for data retention")
args = parser.parse_args()

OPTIONS = [args.sourcedir, args.sourcedir+'/INCREMENT']

INGEST_DATE = []

for i in OPTIONS:
    try:
        list_files = Popen(['hdfs','dfs','-ls',i], stdout=PIPE)
        list_files.wait()
    
        lines = list_files.stdout.read().strip().split('\n')
    except:
        traceback.print_exc()
            
    for p in lines:
        if 'ingest_date' in p:
            INGEST_DATE.append(p)

base_date = (datetime.now() - timedelta(days=int(args.retention))).strftime('%Y-%m-%d')
to_be_deleted = [i.split(' ')[len(i.split(' '))-1] for i in INGEST_DATE if i.split(' ')[len(i.split(' '))-1].split('=')[1] < base_date]

if to_be_deleted:
    print("To be deleted")
    print '\n'.join([i for i in to_be_deleted])
    
    for item in to_be_deleted:
        try:
            delete_folder = Popen(['hdfs','dfs','-rm','-r', item], stdout=PIPE)
            delete_folder.wait()
        except:
            traceback.print_exc()
