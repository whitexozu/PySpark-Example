#-*- coding: utf-8 -*-

import collections
import math
from operator import add
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions
from pyspark.sql.types import *
from pyspark.sql.window import Window
import sys
import operator
import time
import timeit
import threading, requests, time
import numpy as np

class executeFun():
    def getSparkContext(self, appName, master):
        conf = SparkConf().setAppName(appName).setMaster(master)
        return SparkContext(conf=conf)

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
    # reload(sys)
    # sys.setdefaultencoding('utf-8')

    ef = executeFun()
    sc = ef.getSparkContext("createRelXQTable_rdd", sys.argv[1])
    ss = ef.getSparkSession("insert ta_keyword_xq", sys.argv[1])

    createTableSql = []
    createTableSql.append("CREATE TABLE IF NOT EXISTS ta_keyword_xq ")
    createTableSql.append("( ")
    createTableSql.append("c string, ")
    createTableSql.append("t string, ")
    createTableSql.append("xq string ")
    createTableSql.append(") ")
    createTableSql.append("partitioned by (camp_start_dt string, insrcomp_cd string, brch_cd string, spk_cd string) ")
    createTableSql.append("ROW FORMAT DELIMITED ")
    ss.sql(''.join(createTableSql))

    campStartDt = '201709'
    insrcompCdArray = ['51']
    # brchCdArray = ['51', '54', '55']
    brchCdArray = ['51']
    # spkCdArray = ['a', 'f', 'c']
    spkCdArray = ['a']

    a = b = c = d = 0
    tempXq = 0
    tempT = 0

    keywordCList = []
    keywordTList = []
    collectList = []

    # npkeywordCList = np.array([])
    # npkeywordTList = np.array([])
    # npcollectList = np.array([])

    selectQuery = []
    insertRows = []

    compC = 0
    compT = 0

    sf1 = StructField("c", StringType(), False)
    sf2 = StructField("t", StringType(), False)
    sf3 = StructField("xq", StringType(), False)
    sf4 = StructField("camp_start_dt", StringType(), False)
    sf5 = StructField("insrcomp_cd", StringType(), False)
    sf6 = StructField("brch_cd", StringType(), False)
    sf7 = StructField("spk_cd", StringType(), False)
    schema = StructType([sf1, sf2, sf3, sf4, sf5, sf6, sf7])

    tic = timeit.default_timer()
    for insrcompCd in insrcompCdArray:
        for brchCd in brchCdArray:
            for spkCd in spkCdArray:
                selecttic = timeit.default_timer()

                print("--- {0} {1} {2} {3} ---".format(insrcompCd, brchCd, spkCd, sys.argv[2]))

                selectQuery = []
                selectQuery.append("select number, keyword ")
                selectQuery.append("from ( ")
                selectQuery.append("select t2.number as number, t1.keyword as keyword, row_number() over (order by t2.number) as rnum ")
                selectQuery.append("from ( ")
                selectQuery.append("select distinct keyword ")
                selectQuery.append("from ta_common_keyword ")
                selectQuery.append("where camp_start_dt='{0}' ")
                selectQuery.append("and insrcomp_cd='{1}' ")
                selectQuery.append("and brch_cd='{2}' ")
                selectQuery.append("and spk_cd='{3}' ")
                selectQuery.append("and call_type='{4}' ")
                # selectQuery.append("and extract_cd = 'P' ")
                selectQuery.append(") t1 inner join ( ")
                selectQuery.append("select keyword, number ")
                selectQuery.append("from ta_keyword_mapping ")
                selectQuery.append("where camp_start_dt = '{0}' ")
                selectQuery.append(") t2 on t1.keyword = t2.keyword ")
                selectQuery.append(") ")
                selectQuery.append("where rnum % 10 = {5} ")

                selectQuery = (''.join(selectQuery)).format(campStartDt, insrcompCd, brchCd, spkCd, 'sb', sys.argv[2])
                keywordCListDf = ss.sql(selectQuery)

                keywordCRdd = keywordCListDf.rdd.map(lambda p: (p['number'], p['keyword']))

                keywordCList = []
                # npkeywordCList = []
                for t in keywordCRdd.toLocalIterator():
                    keywordCList.append((t[0], t[1]))
                    # npkeywordCList = np.append(npkeywordCList, (t[0], t[1]))
                print("keyword C list size : {0}".format(len(keywordCList)))
                # print("keyword C list size : {0}".format(len(npkeywordCList)))


                selectQuery = []
                selectQuery.append("select t2.number as number, t1.keyword as keyword ")
                selectQuery.append("from ( ")
                selectQuery.append("select distinct keyword ")
                selectQuery.append("from ta_common_keyword ")
                selectQuery.append("where camp_start_dt='{0}' ")
                selectQuery.append("and insrcomp_cd='{1}' ")
                selectQuery.append("and brch_cd='{2}' ")
                selectQuery.append("and spk_cd='{3}' ")
                selectQuery.append("and call_type='{4}' ")
                # selectQuery.append("and extract_cd = 'P' ")
                selectQuery.append(") t1 inner join ( ")
                selectQuery.append("select keyword, number ")
                selectQuery.append("from ta_keyword_mapping ")
                selectQuery.append("where camp_start_dt = '{0}' ")
                selectQuery.append(") t2 on t1.keyword = t2.keyword ")

                selectQuery = (''.join(selectQuery)).format(campStartDt, insrcompCd, brchCd, spkCd, 'sb')
                keywordTListDf = ss.sql(selectQuery)

                keywordTRdd = keywordTListDf.rdd.map(lambda p: (p['number'], p['keyword']))

                keywordTList = []
                # npkeywordTList = []
                for t in keywordTRdd.toLocalIterator():
                    keywordTList.append((t[0], t[1]))
                    # npkeywordTList = np.append(npkeywordTList, (t[0], t[1]))
                print("keyword T list size : {0}".format(len(keywordTList)))
                # print("keyword T list size : {0}".format(len(npkeywordTList)))


                selectQuery = []
                selectQuery.append("with ckl as ( ")
                selectQuery.append("select t1.user_id, collect_set(t2.number) as number_list ")
                selectQuery.append("from ( ")
                selectQuery.append("select user_id, keyword  ")
                selectQuery.append("from ta_common_keyword  ")
                selectQuery.append("where camp_start_dt='{0}' ")
                selectQuery.append("and insrcomp_cd='{1}' ")
                selectQuery.append("and brch_cd='{2}' ")
                selectQuery.append("and spk_cd='{3}' ")
                selectQuery.append("and call_type='{4}' ")
                selectQuery.append(") t1 inner join ( ")
                selectQuery.append("select keyword, number ")
                selectQuery.append("from ta_keyword_mapping ")
                selectQuery.append("where camp_start_dt = '{0}'  ")
                selectQuery.append(") t2 on t1.keyword = t2.keyword ")
                selectQuery.append("group by user_id ")
                selectQuery.append(") ")
                selectQuery.append("select number_list ")
                selectQuery.append("from ckl ")
                selectQuery = (''.join(selectQuery)).format(campStartDt, insrcompCd, brchCd, spkCd, 'sb')
                # print('selectQuery : ' + selectQuery)
                collectListDf = ss.sql(selectQuery)

                collectListRdd = collectListDf.rdd.map(lambda p: (p['number_list']))
                # collectListRdd = collectListDf.rdd.map(lambda p: ','.join(str(e) for e in p['number_list']))

                collectList = []
                npcollectList = []
                for t in collectListRdd.toLocalIterator():
                    collectList.append(t)
                    # npcollectList = np.append(npcollectList, t)
                print("collct list size : {0}".format(len(collectList)))
                # print("collct list size : {0}".format(len(npcollectList)))

                print("--- %s select and append seconds ---" % (timeit.default_timer() - selecttic))

                insertRows = []
                totaltic = timeit.default_timer()

                for keywordC in keywordCList:
                # for keywordC in npkeywordCList:
                    compC = keywordC[0]

                    tempXq = 0
                    tempT = 0

                    keywordCtic = timeit.default_timer()

                    # print(">>>> {0}".format(str(compC)))
                    for keywordT in keywordTList:
                    # for keywordT in npkeywordTList:

                        compT = keywordT[0]

                        if( compC != compT) :

                            keywordTtic = timeit.default_timer()
                            # print(">>>> >>>>> {0}".format(str(compT)))

                            a = b = c = d = 0

                            for collect in collectList:
                                if compC in collect:
                                    if compT in collect:
                                        a += 1
                                    else:
                                        c += 1
                                else:
                                    if compT in collect:
                                        b += 1
                                    else:
                                        d += 1

                            # for collect in npcollectList:
                            #     if np.count_nonzero(collect == compC) > 0:
                            #         if np.count_nonzero(collect == compT) > 0:
                            #             a += 1
                            #         else:
                            #             c += 1
                            #     else:
                            #         if np.count_nonzero(collect == compT) > 0:
                            #             b += 1
                            #         else:
                            #             d += 1

                            totalSize = a + b + c + d
                            numerator = (totalSize * (a * d - c * b) * (a * d - c * b))
                            denominator = ((a + c) * (b + d) * (a + b) * (c + d))
                            if denominator == 0:
                                xq = 0
                            else:
                                xq = numerator / denominator
                                # if xq > tempXq :
                                #     tempXq = xq
                                #     tempT = compT

                            if(xq != 0) :
                                insertRows.append([compC, compT, xq, campStartDt, insrcompCd, brchCd, spkCd])

                            # print("--- {0} {1} {2} ---".format(compC, tempT, tempXq))
                            # print("--- %s keyword T seconds ---" % (timeit.default_timer() - keywordTtic))

                    print("--- %s keyword C seconds ---" % (timeit.default_timer() - keywordCtic))

                # insertDf = ss.createDataFrame(insertRows, schema)
                # insertDf.write.format("orc").insertInto("ta_keyword_xq")

                print("--- %s total process seconds ---" % (timeit.default_timer() - totaltic))

    ss.stop()



# spark-submit createRelXQTableUseMappingNumber.py yarn
