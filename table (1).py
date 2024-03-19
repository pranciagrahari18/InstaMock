import os

import subprocess

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, LongType, DecimalType, TimestampType, LongType, DoubleType

from pyspark.sql.functions import col

import pandas as pd

from datetime import date, datetime

import math

#from storage import storage

import string

import random

from decimal import Context

class table():

    def __init__(self,name, log, session):

        self.name=name

        self.log=log

        self.session=session

        self.database="default"

        self.log.info("Table database: %s"%self.database)

        self.create_table_hive=self.__create_table_hive()
        
        self.table_df=self.__table_df()

        self.location=self.__table_location() 
        self.log.info("Table Location: %s"%self.location)

        self.column_list=self.table_df.columns

        self.spark_schema=self.table_df.schema

        self.partition_columns=[]

        self.__partition_columns()

#        self.column_list=self.column_list [0:(-1)*len(self.partition_columns)] 
#        self.spark_schema=self.spark_schema[0: (-1)*len(self.partition_columns)] 
        log.info("Table Partition Columns: %s" %str(self.partition_columns))

        log.info("Table column list: %s" %str(self.column_list)) 
        log.info("Table Spark Schema: %s" %str(self.spark_schema))

    def __partition_columns (self): 
        table_path=self.create_table_hive

        f=1

        st_ind=table_path.find("PARTITIONED BY") 
        table_path=table_path[st_ind:] 
        st_ind=table_path.find("(")+1 
        end_ind=0

        for i in range(st_ind, len(table_path)):

            if table_path[i]=='(':

                f=f+1

            elif table_path[i]==')':

                f=f-1

            if f==0:
                end_ind=i
                break

        table_path=table_path[st_ind:end_ind] 
        table_path=table_path.replace(" ","")
        table_path=table_path.replace("|","") 
        table_path=table_path.replace("\n","")
 
        lt=table_path.split(",")

        
        for i in range(0, len(lt)): 
            lt[i]=lt[i]

        for i in lt:

            tmp=i.split("`")

            self.partition_columns.append(tmp[0])
            
    def load_data(self, session, data_loc):

        df1=pd.read_csv("/dbfs/FileStore/insta_mock/%s.csv"%self.name, header=0, index_col=None) 
        self.log.info("Pandas Dataframe created")

        path=self.location

        df2=pd.DataFrame([],columns=self.column_list)

        for ind in range(0, len (df1.index)):

            tmp_lt=[]

            for i in range(0, len(self.column_list)):
            

                if self.column_list[i] in df1: 
                    tmp_lt.append(df1[self.column_list[i]][ind]) 

                elif self.spark_schema[i].dataType==IntegerType() or self.spark_schema[i].dataType==LongType(): 
                    tp=random.randint(100,999)

                    tmp_lt.append(tp)

                elif self.spark_schema[i].dataType==DateType() and self.column_list[i]=='end dt':

                    tmp_lt.append(date (2099,1,1))

                elif self.spark_schema[i].dataType==DateType():

                    tmp_lt.append(date (2023,1,1))

                elif self.spark_schema[i].dataType==TimestampType(): 
                    tmp_lt.append(datetime (2099,1,1,0,0,0,0))


                elif self.spark_schema[i].dataType==StringType():
                    letters=string.ascii_lowercase

                    tp=''.join(random.choice (letters) for k in range(3))

                    tmp_lt.append(tp)

                else:

                    #self.type_

                    tp=random.uniform(40,80)

                    tp=Context(prec=38).create_decimal(tp)

                    tmp_lt.append(tp)

            df2.loc[ind]=tmp_lt
            
        self.log.info("Pandas Dataframe created adding default values") 
        df=session.createDataFrame(df2, schema=self.spark_schema) 
        self.log.info("Spark Dataframe created from pandas dataframe")

        df.write.partitionBy(self.partition_columns).mode('append').save(path)

    def __create_table_hive(self):

        try:

            df=self.session.sql("show create table %s.%s" %(self.database, self.name)) 
            str=df._jdf.showString(1,100000000, False) 
            return str

        except:

            self.log.info("Wrong table information as per metadata") 
            raise Exception("wrong table information")

    def __table_df(self):

        df=self.session.sql("select * from %s.%s" %(self.database, self.name))

        return df

    def __table_location(self):

        table_path=self.create_table_hive

        lt=table_path.split("\n")

        n=len (lt)
        table_path=""
        for i in range(0,n):

            if lt[n-1-i].find("LOCATION") !=-1:

                st_ind=lt[n-1-i].find("'")+1

                end_ind=lt[n-1-i][st_ind+1:].find("'")+st_ind+1 
                table_path=lt[n-1-i][st_ind: end_ind]
        
        return table_path
"""
    @staticmethod
    def create_table(name, env, region, part type, strg, partition_columns, session):

        bdp_columns=[

        ["bdp_sor_extracttimestamp","timestamp"],

        ["bdp_actiontype", "string"],

        ["bdp_processcode", "string"],

        ["bdp_processmessage","string"], ["bdp_createtimestamp", "timestamp"],

        ["bdp_rowstatus", "string"],

        ["bdp_updatetimestamp","timestamp"]

        ]

        df_temp=pd.read_csv("attribute_"+name+"_operation.csv", header=0, index_col=None)

        df_atr=pd.DataFrame({

        "name": df_temp["name"],

        "type":df_temp["type"],

        "format":df_temp["format"]

        }) 
        
        if region in ("landing", "raw") and part_type== "day":

            for i in bdp_columns:

                df_atr.loc[len(df_atr.index)]={"name":i[0],

            "type":i[1],

            "format":pd.np.nan}

        if part_type=="custom":

            del_list=[]

            for j in range(0, len(df_atr.index)):

                if df atr.iloc[j]["name"] in partition_columns:

                    del_list.append(j)

            df_atr.drop(del_list,axis=0, inplace=True)
        
        print(df_atr)

        database="bdp"+env[0]+env[-2:]+region[0:3] 
        crt_str="create external table %s.%s ("%(database, name) 
        
        for i in range(0, len(df_atr.index)):

            crt_str=crt_str+df_atr.iloc[i]["name"]+" " 
            crt_str=crt_str+df_atr.iloc[i]["type" ]

            if (not pd.isnull(df_atr.iloc[i]["format"])) and df_atr.iloc[i]["type"] not in ('Int', 'Date', "Timestamp"): 
                crt_str+crt_str+"("+str(int(df_atr.iloc[i]["format"]))+") "

            crt_str=crt_str+","

        crt_str=crt_str[:-1]

        crt strecrt str+") partitioned by ( "

        for i in partition_columns:

            crt_str=crt str+i+" string ,"

        crt_str=crt_str[:-1]

        if strg=="hdfs":

            crt_str=crt_str+") location 'hdfs://nonpdp01/%s/%s/%s/%s/' "% (env, region, name[0:6], name)

        else:

            crt str=crt_str+") location abfss://adls@paea3staccadls001.dfs.core.windows.net/OpsHDFS/%s/%s/%s/%s/"%(env, region, name[0:6], name)

        if region in ("landing", "raw"): 
            crt_str=crt_str+"processed/" 
            
        crt_str=crt_str+"'"

        session.spark(crt_str)

        print crt_str"""