#-*- coding: utf-8 -*-

import collections
from operator import add
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions
from pyspark.sql.types import *
from pyspark.sql.window import Window
import sys

class executeFun():
    def getSparkSession(self, appName, master):
        ss = SparkSession \
            .builder \
            .appName(appName) \
            .master(master) \
            .config("spark.sql.warehouse.dir", "file:///tmp/spark/Temp") \
            .enableHiveSupport() \
            .getOrCreate()
        return ss

if __name__ == "__main__":
    ef = executeFun()
    ss = ef.getSparkSession("create ta_common_keyword table", sys.argv[1])

    campStartDt = sys.argv[2]
    insrcompCdArray = ['51']
    brchCdArray = ['51', '54', '55']
    spkCdArray = ['f', 'c', 'a']
    callTypeArray = ['sb', 'nsb']

    createTableSql = []
    createTableSql.append("CREATE EXTERNAL TABLE IF NOT EXISTS ta_common_keyword ")
    createTableSql.append("( ")
    createTableSql.append("user_id string, ")
    createTableSql.append("keyword string, ")
    createTableSql.append("tf int, ")
    createTableSql.append("call_id string ")
    createTableSql.append(") ")
    createTableSql.append("partitioned by (camp_start_dt string, insrcomp_cd string, brch_cd string, spk_cd string, call_type string) ")
    createTableSql.append("ROW FORMAT DELIMITED ")
    createTableSql.append("FIELDS TERMINATED BY '|' ")
    createTableSql.append("stored as textfile ")
    createTableSql.append("location '/dq/skm/common/keyword/daily' ")

    ss.sql(''.join(createTableSql))

    for insrcompCd in insrcompCdArray:
        for brchCd in brchCdArray:
            for spkCd in spkCdArray:
                for callType in callTypeArray:
                    ss.sql("ALTER TABLE ta_common_keyword ADD PARTITION (camp_start_dt='{0}', insrcomp_cd='{1}', brch_cd='{2}', spk_cd='{3}', call_type='{4}') location '/dq/skm/common/keyword/daily/{0}/{1}/{2}/{3}/{4}'".format(campStartDt, insrcompCd, brchCd, spkCd, callType))

    createTableSql = []
    createTableSql.append("CREATE EXTERNAL TABLE IF NOT EXISTS ta_common_user ")
    createTableSql.append("( ")
    createTableSql.append("user_id string, ")
    createTableSql.append("gender string, ")
    createTableSql.append("age int, ")
    createTableSql.append("loc string, ")
    createTableSql.append("call_time_a string, ")
    createTableSql.append("call_time_c string, ")
    createTableSql.append("call_time_f string, ")
    createTableSql.append("call_rslt_cd string, ")
    createTableSql.append("emo_pos int, ")
    createTableSql.append("emo_eng int, ")
    createTableSql.append("call_id string ")
    createTableSql.append(") ")
    createTableSql.append("partitioned by (camp_start_dt string, insrcomp_cd string, brch_cd string, call_type string) ")
    createTableSql.append("ROW FORMAT DELIMITED ")
    createTableSql.append("FIELDS TERMINATED BY '|' ")
    createTableSql.append("stored as textfile ")
    createTableSql.append("location '/dq/skm/common/member/daily' ")

    ss.sql(''.join(createTableSql))

    for insrcompCd in insrcompCdArray:
        for brchCd in brchCdArray:
            for callType in callTypeArray:
                ss.sql("ALTER TABLE ta_common_user ADD PARTITION (camp_start_dt='{0}', insrcomp_cd='{1}', brch_cd='{2}', call_type='{3}') location '/dq/skm/common/member/daily/{0}/{1}/{2}/{3}'".format(campStartDt, insrcompCd, brchCd, callType))

    ss.stop()

# spark-submit createHiveTable.py yarn 201709