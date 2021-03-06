#!/usr/bin/env python

import sys
import json
import argparse
from collections import OrderedDict
import os
import shutil
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date
from jinja2 import Environment, PackageLoader, select_autoescape

templates = Environment(
    loader=PackageLoader('dataengineer_toolkit.job_generator', 'templates'),
    autoescape=select_autoescape('html', 'xml')
)

hive_create_template = templates.get_template('hive.sql.j2')

falcon_process_template = templates.get_template('falconprocess.xml.j2')

falcon_feed_template = templates.get_template('falconfeed.xml.j2')

falcon_hivefeed_template = templates.get_template('falconhivefeed.xml.j2')

oozie_properties = OrderedDict([
    ('resourceManager', 'hdpmaster1.tm.com.my:8050'),
    ('jobTracker', 'hdpmaster1.tm.com.my:8050'),
    ('nameNode', 'hdfs://hdpmaster1.tm.com.my:8020'),
#    ('hivejdbc', 'jdbc:hive2://hdpmaster1.tm.com.my:10000/default'),
    ('oozie.wf.application.path', '/user/trace/workflows/%(workflow)s/'),
    ('oozie.use.system.libpath', 'true'),
    ('prefix', None),
    ('jdbc_uri', 'jdbc:oracle:thin:@%(host)s:%(port)s/%(tns)s'),
    ('username', None),
    ('password', None),
    ('source_name', None),
    ('direct', None),
    ('targetdb', None),
    ('stagingdb', None),
    ('backdate', '7'),
    ('schema', None),
    ('table', None),
    ('mapper', None),
    ('split_by', None),
    ('merge_column', None),
    ('check_column', None),
#    ('columns', None),
#    ('columns_create', None),
    ('columns_java', None),
])

JAVA_TYPE_MAP = {
#    'VARCHAR2': 'String',
    'DATE': 'String',
#    'NUMBER': 'String',
#    'CHAR': 'String',
#    'LONG': 'Long',
}

STAGES = {
   'dev': {
      'prefix': '/user/trace/development/',
      'targetdb': '%(source_name)s_DEV',
      'stagingdb': 'staging_dev',
   },
   'test': {
      'prefix': '/user/trace/test/',
      'targetdb': '%(source_name)s',
      'stagingdb': 'staging_test',
   },
   'prod': {
      'prefix': '/user/trace/',
      'targetdb': '%(source_name)s',
      'stagingdb': 'staging',
   }
}

PROCESSES = {
    'ingest-full': {
        'workflow': 'ingest-full',
        'out_feeds': ['full'],
        'condition': lambda x: x['merge_column'] and x['check_column'],
    },
    'ingest-increment': {
        'workflow': 'ingest-increment',
        'out_feeds': ['increment'],
    },
    'transform-full': {
        'in_feeds': ['full'],
        'workflow': 'transform-full',
    },
    'transform-increment': {
        'in_feeds': ['increment'],
        'workflow': 'transform-increment',
    },
    'incremental-ingest-frozen': {
        'workflow': 'incremental-ingest-frozen',
    }
}

EXEC_TIME = {
    'CPC': {
        'ingest-full': '00:01',
        'ingest-increment': '00:01',
        'transform-full': '00:30',
        'transform-increment': '00:30'
    },
    'SIEBEL_NOVA': {
        'ingest-full': '03:00',
        'ingest-increment': '03:00',
        'transform-full': '05:00',
        'transform-increment': '05:00'
    },
    'BRM_NOVA': {
        'ingest-full': '03:01',
        'ingest-increment': '03:01',
        'transform-full': '05:00',
        'transform-increment': '05:00'
    },
    'GRANITE': {
        'ingest-full': '03:01',
        'ingest-increment': '03:01',
        'transform-full': '05:00',
        'transform-increment': '05:00'
    },
    'NIS': {
        'ingest-full': '03:01',
        'ingest-increment': '03:01',
        'transform-full': '05:00',
        'transform-increment': '05:00'
    },
    'PORTAL': {
        'ingest-full': '03:01',
        'ingest-increment': '03:01',
        'transform-full': '05:00',
        'transform-increment': '05:00'
    }
}

FEEDS = {
   'full-retention': {
       'path': '%(prefix)s/source/%(source_name)s/%(schema)s_%(table)s/ingest_date=${YEAR}-${MONTH}-${DAY}',
       'format': 'parquet',
       'exec_time': '00:00',
       'retention': 365
   },
   'increment-retention': {
        'path': '%(prefix)s/source/%(source_name)s/%(schema)s_%(table)s/INCREMENT/ingest_date=${YEAR}-${MONTH}-${DAY}',
        'format': 'parquet',
        'exec_time': '00:00',
        'retention': 365
   },
   'full': {
       'path': '%(prefix)s/source/%(source_name)s/%(schema)s_%(table)s/CURRENT/',
       'format': 'parquet',
       'exec_time': '00:00',
   },
   'increment': {
        'path': '%(prefix)s/source/%(source_name)s/%(schema)s_%(table)s/INCREMENT/CURRENT',
        'format': 'parquet',
        'exec_time': '00:00'
   },
}

HIVE_FEEDS = {
    'hive-retention' : {
        'path': 'catalog:%(targetdb)s_HISTORY:%(schema)s_%(table)s#ingest_date=${YEAR}-${MONTH}-${DAY}',
        'format': 'orc',
        'exec_time': '00:00',
        'retention': 365
    }
}

ARTIFACTS='artifacts/'

FOREVER=36135

def get_exec_time(source, process):
    return EXEC_TIME.get(source, {}).get(process, '03:01')

def generate_utc_time(t, dayoffset=0):
    tt = (datetime.now() + timedelta(days=dayoffset)).strftime('%Y-%m-%d')
    dt = tt + ' %s' % t
    start_dt = parse_date(dt)
    return (start_dt - timedelta(hours=8)).strftime('%Y-%m-%dT%H:%MZ')

def default_feed_name(stage, properties, feed):
    return (stage + 
        '-%(source_name)s-%(schema)s-%(table)s-' % properties +
        feed
    ).replace('_','')

def default_process_name(stage, properties):
    return (
        stage + 
        '-%(source_name)s-%(schema)s-%(table)s-%(workflow)s' % properties
    ).replace('_','')

def write_oozie_config(storedir, properties):
    filename = '%(source_name)s-%(schema)s-%(table)s.properties' % properties
    if not os.path.exists(storedir):
        os.makedirs(storedir)
    with open('%s/%s' % (storedir, filename), 'w') as f:
        job = '\n'.join([
            '%s=%s' % (k,v) for k,v in oozie_config(properties).items()
        ])
        f.write(job)

def oozie_config(properties):
    prop = oozie_properties.copy()
    prop['jdbc_uri'] = prop['jdbc_uri'] % properties
    prop['oozie.wf.application.path'] = properties['wfpath']
    for k,v in prop.items():
        if k in properties.keys():
            prop[k] = properties[k]
    prop['appName'] = (
        properties.get('appName', None) or 
        '%(workflow)s-%(source_name)s-%(schema)s-%(table)s' % properties
    )
    return prop

def write_falcon_process(storedir, stage, properties, in_feeds=None,
        out_feeds=None, process_time='05:00'):
    filename = '%(source_name)s-%(schema)s-%(table)s.xml' % properties
    if not os.path.exists(storedir):
        os.makedirs(storedir)
    with open('%s/%s' % (storedir, filename), 'w') as f:
        params, job = falcon_process(stage, properties, in_feeds, out_feeds,
                process_time)
        f.write(job)

   

def falcon_process(stage, properties, in_feeds=None, out_feeds=None,
        process_time='05:00'):
    inputs = [
        default_feed_name(stage, properties, 
            i) for  i in (in_feeds or [])
        ]
    outputs = [
        default_feed_name(stage, properties, 
            i) for  i in (out_feeds or [])
        ]
    inputs_xml = '\n        '.join(
            ['<input name="input%s" feed="%s" start="today(8,0)" end="today(9,30)"/>' % 
                (t,i) for t,i in enumerate(inputs)])
    inputs_xml = '<inputs>%s</inputs>' % inputs_xml if inputs_xml else ''
    outputs_xml = '\n        '.join(
            ['<output name="output%s" feed="%s" instance="today(8,0)"/>' %
                (t,i) for t,i in enumerate(outputs)])
    outputs_xml = '<outputs>%s</outputs>' % outputs_xml if outputs_xml else ''

    prop = oozie_config(properties)
    params = {
        'schema': properties['schema'],
        'table': properties['table'],
        'workflow': properties['workflow'],
        'source_name': properties['source_name'],
        'start_utc': generate_utc_time(process_time, dayoffset=1),
        'should_end_hours': 5,
        'frequency_hours': 24,
        'process_name': default_process_name(stage, properties),
        'workflow_path': prop['oozie.wf.application.path'],
        'workflow_name': prop['appName'].replace('_',''),
        'stage' : stage,
        'properties': '\n       '.join(
            ['<property name="%s" value="%s"/>' % (k,v) for (k,v) in prop.items() if '.' not in k]),
        'outputs': outputs_xml,
        'inputs': inputs_xml,
    }
    job = falcon_process_template.render(**params)
    return params, job


def write_falcon_feed(storedir, stage, properties, feed, feed_path, 
        feed_format, exec_time='00:00', retention=FOREVER):
    filename = '%(source_name)s-%(schema)s-%(table)s.xml' % properties
    if not os.path.exists(storedir):
        os.makedirs(storedir)
    with open('%s/%s' % (storedir, filename), 'w') as f:
        params, job = falcon_feed(stage, properties, feed, 
                    feed_path, feed_format, exec_time, retention)
        f.write(job)

def falcon_feed(stage, properties, feed, feed_path, feed_format, 
            exec_time='00:00', retention=FOREVER):
    if retention is not None:
        rt = "<retention limit='days(%s)' action='delete'/>" % retention
    else:
        rt = ''
    params = {
       'schema': properties['schema'],
       'table': properties['table'],
       'source_name': properties['source_name'],
       'start_utc': generate_utc_time(exec_time),
       'feed_name': default_feed_name(stage, properties, feed), 
       'feed_path': feed_path % properties,
       'feed_type': feed,
       'feed_format': feed_format,
       'stage': stage,
       'retention': rt
    }
    job = falcon_feed_template.render(**params)
    return params, job

def write_falcon_hivefeed(storedir, stage, properties, feed, feed_path, 
        feed_format, exec_time='00:00', retention=FOREVER):
    filename = '%(source_name)s-%(schema)s-%(table)s.xml' % properties
    if not os.path.exists(storedir):
        os.makedirs(storedir)
    with open('%s/%s' % (storedir, filename), 'w') as f:
        params, job = falcon_hivefeed(stage, properties, feed, 
                    feed_path, feed_format, exec_time, retention)
        f.write(job)

def falcon_hivefeed(stage, properties, feed, feed_path, feed_format, 
            exec_time='00:00', retention=FOREVER):
    if retention is not None:
        rt = "<retention limit='days(%s)' action='delete'/>" % retention
    else:
        rt = ''
    params = {
       'schema': properties['schema'],
       'table': properties['table'],
       'source_name': properties['source_name'],
       'start_utc': generate_utc_time(exec_time),
       'feed_name': default_feed_name(stage, properties, feed), 
       'feed_path': feed_path % properties,
       'feed_type': feed,
       'feed_format': feed_format,
       'stage': stage,
       'retention': rt
    }
    job = falcon_hivefeed_template.render(**params)
    return params, job


def main():
    argparser = argparse.ArgumentParser(description='Generate oozie and falcon configurations for ingestion')
    argparser.add_argument('profilerjson', help='JSON output from oracle_profiler.py')
    opts = argparser.parse_args()
    hive_create = []
    import csv

    if os.path.exists(ARTIFACTS):
        shutil.rmtree(ARTIFACTS)
    for ds in json.loads(open(opts.profilerjson).read()):
        for table in ds['tables']:

            mapper = int((table['estimated_size'] or 0) / 1024 / 1024 / 1024) or 2
            if mapper < 2:
                mapper = 2
            if mapper > 25:
                mapper = 25
            columns = [c['field'] for c in table['columns']]
            columns_create = []
            columns_java = []
            for c in table['columns']:
                if JAVA_TYPE_MAP.get(c['type'], None):
                    columns_java.append('%s=%s' % (c['field'], JAVA_TYPE_MAP[c['type']]))

            source_name = ds['datasource']['name'].replace(' ','_')
            username = ds['datasource']['login']
            password = ds['datasource']['password']

            params = {
                'mapper': mapper, 
                'source_name': source_name,
                'host': ds['datasource']['ip'],
                'port': ds['datasource']['port'],
                'username': username, # ds['datasource']['login'],
                'password': password,
                'tns': ds['datasource']['tns'],
                'schema': ds['datasource']['schema'],
                'table': table['table'],
                'split_by': table['split_by'],
                'columns_java': ','.join(columns_java),
                'columns_create': ','.join(columns_create),
                'columns_create_newline': ',\n    '.join(columns_create),
                'columns': ','.join(['`%s`' % c['field'] for c in table['columns']]),
                'merge_column': table['merge_key'],
                'check_column': table['check_column'],
                'direct': ds['direct']
            }
   
            for stage, conf in STAGES.items():

                if stage in ['prod']:
                    opts = params.copy()
                    opts['stagingdb'] = conf['stagingdb']
                    opts['targetdb'] = conf['targetdb'] % params
                    opts['prefix'] = conf['prefix']
                    hive_create.append(
                        hive_create_template.render(**opts)
                    )
    
                for process, proc_opts in PROCESSES.items():
                    opts = params.copy()
                    opts['prefix'] = conf['prefix']
                    opts['targetdb'] = conf['targetdb'] % params
                    opts['stagingdb'] = conf['stagingdb'] 

                    wf = proc_opts['workflow']
                    opts['workflow'] = wf
                    opts['wfpath'] = os.path.join(conf['prefix'],'workflows', wf)
                    if not proc_opts.get('condition', lambda x: True):
                        continue

                    storedir = '%s/%s-oozie-%s' % (ARTIFACTS, stage, wf)
                    write_oozie_config(storedir, opts)
                    storedir = '%s/%s-falconprocess-%s' % (ARTIFACTS, stage, wf)
                    write_falcon_process(storedir, stage, opts,
                        proc_opts.get('in_feeds', []), 
                        proc_opts.get('out_feeds', []),
                        get_exec_time(opts['source_name'], process)
                    )

                for feed, feed_opts in FEEDS.items():
                    opts = params.copy()
                    opts['prefix'] = conf['prefix']
                    storedir = '%s/%s-falconfeed-%s' % (ARTIFACTS, stage, feed)
                    write_falcon_feed(storedir, stage, opts, feed,
                                    feed_opts['path'], feed_opts['format'],
                                    feed_opts['exec_time'],
                                    feed_opts.get('retention', FOREVER))

                for feed, feed_opts in HIVE_FEEDS.items():
                    opts = params.copy()
                    opts['prefix'] = conf['prefix']
                    opts['targetdb'] = conf['targetdb'] % params
                    storedir = '%s/%s-falconfeed-%s' % (ARTIFACTS, stage, feed)
                    write_falcon_hivefeed(storedir, stage, opts, feed,
                                    feed_opts['path'], feed_opts['format'],
                                    feed_opts['exec_time'],
                                    feed_opts.get('retention', FOREVER))


    open('hive-create.sql', 'w').write('\n'.join(hive_create))

if __name__ == '__main__':
    main()
