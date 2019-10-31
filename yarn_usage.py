#!/usr/bin/env python
# Author : Adi Yusman
# Email : adiyusman.yusof@cimb.com
# Date Modified : 2019-04-26
# Sample command : python yarn-usage.py --settings settings.yml --start yyyy-mm-dd --end yyyy-mm-dd
# Purpose:  to get list of all queries or job executed in hive for cluster user
#           usage. cloudera manager yarn > applications API 
###############################################################################

import csv
import urllib 
import json
import argparse
import time
import yaml

class Config(object):

    value = ''

    def __init__(self, config_file):
        with open(config_file, 'r') as stream:
            try:
                self.value = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

def retrieve_data(apps):
    # Filter required data
    appID = apps['applicationId']
    name = apps['name'].replace('\n',' ').replace('\r',' ')
    user = apps['user']
    pool = apps['pool']
    state = apps['state']
    startTime = apps['startTime']
    endTime = apps['endTime']

    # Handle hive query key not exists
    if 'hive_query_string' in apps['attributes']:
        hiveString = apps['attributes']['hive_query_string'].replace('\n',' ').replace('\r',' ')
    else:
        hiveString = ''
    data=[appID,name,user,pool,state,startTime,endTime,hiveString]
    return data

def dump_csv(data,filename):
    # Dump data to csv
    with open(filename, mode='a') as outfile:
        outfile_writer = csv.writer(outfile, delimiter='|', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        outfile_writer.writerow(data)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="start date in 'yyyy-mm-dd' format",dest="start")
    parser.add_argument("--end", help="end date in 'yyyy-mm-dd' format",dest="end")
    parser.add_argument("--settings", help="Python YAML settings file",dest="settings")
    args = parser.parse_args()
    
    startTime = args.start
    endTime = args.end
    settings = args.settings

    config = Config(settings)
    yarn = config.value['yarn']

    if startTime <= endTime:
        url = "https://"+yarn['hostname']+":7183/api/v7/clusters/"+yarn['cluster']+"/services/yarn/yarnApplications?from="+startTime+"T07:00:59.321Z&to="+endTime+"T07:00:59.321Z&filter=pool=root."+yarn['queue']+"&limit=1000" 
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())
        
        # Prepare output file
        path = 'outfile/'
        filename = path+'outfile-'+time.strftime("%Y%m%d%H%M")+'.csv'
        with open(filename, mode='w') as outfile:
            outfile_writer = csv.writer(outfile, delimiter='|', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            outfile_writer.writerow(['APPID', 'NAME','USER','POOL','STATE','STARTTIME','ENDTIME','HIVESTRING'])

        # Start Process
        for apps in data['applications']:
            data = retrieve_data(apps)
            dump_csv(data,filename)
            
    else:
        print("Enter a valid dates input")

if __name__ == '__main__':
    main()

