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
import requests
import time
import datetime
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

    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S%f')
    URL = 'http://localhost:8080/TA_SKM/ta/openApi/insertMntrLog.do'
    data = {'jobId': st, 'key': 'WT', 'type': 'M', 'state': 'S', 'serviceKey': 'b7a9dce1-9a39-4382-8b94-910220b5'}
    res = requests.post(URL, data=data)

    ef = executeFun()
    ss = ef.getSparkSession("insert ta_keyword_tfidf", sys.argv[1])

    # # 캠페인별 키워드 데이터를 TF, DF, user_count 형식으로 조회
    # selectTfidfDf = ss.sql("select keyword, sum(tf) as tf, count(call_id) as df, count(distinct user_id) as user_count from ta_common_keyword group by keyword")
    # selectTfidfDfRows = selectTfidfDf.rdd.map(lambda x: [x.keyword, x.tf, x.df, x.user_count])
    #
    # # 조회된 데이터를 partition 단위로 저장
    # tfidfSf1 = StructField("keyword", StringType(), True)
    # tfidfSf2 = StructField("tf", IntegerType(), True)
    # tfidfSf3 = StructField("df", IntegerType(), True)
    # tfidfSf4 = StructField("user_count", IntegerType(), True)
    # tfidfSchema = StructType([tfidfSf1, tfidfSf2, tfidfSf3, tfidfSf4])
    # ss.sql("CREATE TABLE IF NOT EXISTS ta_word_tfidf (keyword string, tf int, df int, user_count int)")
    # insertTfidfDf = ss.createDataFrame(selectTfidfDfRows, tfidfSchema)
    # insertTfidfDf.write.mode("overwrite").format("orc").saveAsTable("ta_word_tfidf")
    #
    # ss.sql("REFRESH TABLE ta_word_tfidf")
    #
    # ss.stop()

    createTableSql = []
    createTableSql.append("CREATE TABLE IF NOT EXISTS ta_keyword_tfidf ")
    createTableSql.append("( ")
    createTableSql.append("keyword string, ")
    createTableSql.append("tf int, ")
    createTableSql.append("df int, ")
    createTableSql.append("user_count int ")
    createTableSql.append(") ")
    createTableSql.append("partitioned by (camp_start_dt string, insrcomp_cd string, brch_cd string, spk_cd string, call_type string) ")
    createTableSql.append("ROW FORMAT DELIMITED ")

    ss.sql(''.join(createTableSql))

    campStartDt = '201709'
    insrcompCdArray = ['51']
    brchCdArray = ['51', '54', '55']
    spkCdArray = ['f', 'c', 'a']
    callTypeCdArray = ['sb', 'nsb']

    try:
        for insrcompCd in insrcompCdArray:
            for brchCd in brchCdArray:
                for spkCd in spkCdArray:
                    for callTypeCd in callTypeCdArray:
                        ss.sql("INSERT INTO ta_keyword_tfidf PARTITION (camp_start_dt='{0}', insrcomp_cd='{1}', brch_cd='{2}', spk_cd='{3}', call_type='{4}') select keyword, sum(tf) as tf, count(call_id) as df, count(distinct user_id) as user_count from ta_common_keyword where camp_start_dt='{0}' and insrcomp_cd='{1}' and brch_cd='{2}' and spk_cd='{3}' and call_type='{4}' group by keyword".format(campStartDt, insrcompCd, brchCd, spkCd, callTypeCd))

        data = {'jobId': st, 'key': 'WT', 'type': 'M', 'state': 'E', 'serviceKey': 'b7a9dce1-9a39-4382-8b94-910220b5'}
        res = requests.post(URL, data=data)
    except BaseException as e:
        print(e)
        data = {'jobId': st, 'key': 'WT', 'type': 'M', 'state': 'B', 'serviceKey': 'b7a9dce1-9a39-4382-8b94-910220b5'}
        res = requests.post(URL, data=data)
    finally:
        ss.stop()


# spark-submit --master yarn \
# --deploy-mode client \
# --executor-memory 2g \
# --name HdfsToHive \
# --conf "spark.app.id=HdfsToHive" \
# createTFIDFTable.py \
# yarn

# spark-submit createTFIDFTable.py yarn