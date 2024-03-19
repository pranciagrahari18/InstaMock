from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, LongType, DecimalType, TimestampType
import sys 

sys.path.insert(1,"/dbfs/FileStore/insta_mock/")
import os
from table import table
from logger import logger
import pandas as pd
import random 
import string

def nextWord(s):
    if s=="":
        return 'a'

    i=len(s)-1

    while s[i]=='z' and i>=0: 
        s=s[0:1]+'a'+s[i+1:]
        i=i-1
    if i==-1:
        s+='a'
    else:
        s=s[0:i]+chr(ord(s[i])+1) + s[i+1:] 
    return s

session=SparkSession.builder.enableHiveSupport().getOrCreate()

operation=sys.argv[1]

name=sys.argv[2]

log=logger(name)

log.info("Operation selected: %s"%operation)

if operation=="insert":

#    env=sys.argv[3]

#    region=sys.argv[4]

#    partitions=sys.argv[5:]

    log.info("arguments : %s"%str({"name": name}))

    log.info("creating table instance")

    tb=table(name,log,session)

    log.info("starting data load in table")
    
    print(tb.location)
    print(tb.spark_schema)
    print(tb.location)
    print(tb.partition_columns)

    tb.load_data(session, "%s.csv"%name)

    log.info("bata loaded into table successfully")

elif operation.startswith("insert-"):

    fill=int(operation[7:])

    env=sys.argv[3]

    region=sys.argv[4]

    primary_key=sys.argv[5].split(' ')

    partitions=sys.argv[6:]

    log.info("arguments: %s "%str({"name": name, "env": env, "region": region, "partitions": partitions}))

    log.info("creating table instance")

    tb=table(name, env,region,log, session)

    df=pd.read_csv("%s.csv"%name, header=0, index_col=None)

    ni=len(df.index)

    letters=string.ascii_lowercase

    primary_val=[]

    val_check=[]

    for i in df.columns:

        val_check.append(df[i].values)

    for i in range (0,len(primary_key)):
        for j in tb.spark_schema:
            if primary_key[i]==j.name:
                if j.dataType==IntegerType() or j.dataType==LongType() or j.dataType==DecimalType():
                    primary_val.append(0)
                else:
                    primary_val.append("")
                break

    log.info("adshabadhahdjhs d"%len(df.index))

    for i in range(0, fill-len(df.index)):

        lt=[]

        #log.info("adahdbadhshdjha")

        for j in range(0,len(df.columns)):

            if df.columns[j] in primary_key:

                if tb.spark_schema[j].dataType==IntegerType() or tb.spark_schema[j].dataType==LongType() or tb.spark_schema[j].dataType==DecimalType():

                    primary_val[j]=primary_val[j]+1

                    while primary_val[j] in val_check[j]:

                        primary_val[j]=primary_val[j]+1

#                    1t.append(primary_val[j])

                else:

                    primary_val[j]=nextWord(primary_val[j])

                    while df.iloc[0][i][:(-1)*len(primary_val[j])]+primary_val[j] in val_check[j]:
                        primary_val[j]=nextWord(primary_val[j])

#                    1t.append(df.iloc[0][j][:(-1)*len(primary_val[j])]+primary_val[j])

            else:

                tp=df.iloc[i%ni][j]

                lt.append(tp)

        df.loc[len(df.index)]=lt

    df.to_csv("%s.csv"%name, header=True, index=False)

    tb.load_data(session, "%s.csv"%name,partitions)

    log.info("Data loaded into table successfully")

elif operation=="create-table":

    env=sys.argv[3]

    region=sys.argv[4]

    cluster=sys.argv[5]

    part_type=sys.argv[6]

    if part_type=="custom":

        partition_columns=sys.argv[8:]

    else:

        partition_columns=["bdp_year","bdp_month","bdp_day", "bdp hour"]

    log.info("argumenta: %s"%str({"name":name, "env" :env, "region":region, "cluster": cluster, "partition type":part_type, "partition column=": partition_columns}))

    table.create_table(name, env, region,part_type, cluster,partition_columns, session)

elif operation=="update":

    env=sys.argv[3]

    region=sys.argv[4]

    primary_key=sys.argv[5]

    partitions=Sys.argv[6:]

    log.info("arguments: %s"%str({"name": name, "env": env, "region": region, "primary key": primary_key, "partitions": partitions}))

    tb=table(name, env, region, log)

    log.info("Table Location: "%tb.location)

    log.info("Table Spark schema: %s"%str(tb.spark_schema))

    log.info("Table Spark Schema: %s"%str(tb.column_list))

    tb.update(session, "%s.csv"%name, primary_key,partitions)

elif operation=="delete":

    env=sys.argv[3]

    region=sys.argv[4]

    partitions=sys.argv[6:]

    log.info("arguments: %s"%str({"name":name, "env" :env, "region": region, "partitions": partitions}))

    tb=table(name, env, region, log)

    log.info("table location: %s"%tb.location)

    log.info("Table Spark Schema: %s"%str(tb.spark_schema))

    log.info("Table Spark Schema: %s"%str(tb.column_list))
    tb.delete(partitions)
    
else:
    log.info("Error: chose correct options")

