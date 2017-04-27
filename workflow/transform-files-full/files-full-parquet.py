import argparse

if not 'sc' in globals():
    from pyspark.context import SparkContext
    from pyspark.sql import HiveContext

    sc = SparkContext(appName='FilesToParquet')
    sqlContext = HiveContext(sc)

parser = argparse.ArgumentParser()
parser.add_argument("--sourcedir", help="source directory")
parser.add_argument("--outputdir", help="output directory")
parser.add_argument("--ingestdate", help="ingest date")
parser.add_argument("--schema", help="Schema")
parser.add_argument("--db", help="database")
parser.add_argument("--table", help="table")
args = parser.parse_args()

options = {
        'sourcedir':args.sourcedir,
        'outputdir':args.outputdir,
        'ingestdate':args.ingestdate,
        'schema':args.schema,
        'db':args.db,
        'table':args.table,
        }

def process(record,schema):
    split_text = []
    if schema == 'OSM':
        d = "1-"
        for i,e in enumerate(record.split("\n1-")):
            if i > 0:
                split_text.append(d+e)
            else:
                split_text.append(e)
    else:
        split_text = [row for row in record.split("\n") if row]
    return split_text

f = sc.wholeTextFiles('%(sourcedir)s/RAW/ingest_date=%(ingestdate)s' % options)
schema = options['schema']
stage1 = f.collect()
split_text = process(stage1[0][1],schema)
header = split_text[0].split("~^")
split_text.pop(0)
if split_text:
    rows = [row.split("~^") for row in split_text if row]

df_writer = sqlContext.createDataFrame(rows,header)

df_writer.registerTempTable("df_writer")
sqlContext.sql("create database if not exists ingest_%(db)s" % options)
sqlContext.sql("create table if not exists ingest_%(db)s.%(schema)s_%(table)s_schema \
                    row format serde \
                    'org.apache.hadoop.hive.serde2.avro.AvroSerDe' \
                stored as avro \
                tblproperties ( \
                    'avro.schema.url'='%(sourcedir)s/PARQUET/.metadata/schema.avsc' \
                )" % options)
sqlContext.sql("create external table if not exists \
                ingest_%(db)s.%(schema)s_%(table)s_current \
                like ingest_%(db)s.%(schema)s_%(table)s_schema \
                stored as parquet \
                location '%(outputdir)s/CURRENT'" % options)
sqlContext.sql("insert overwrite table ingest_%(db)s.%(schema)s_%(table)s_current \
        select * from df_writer" % options)
