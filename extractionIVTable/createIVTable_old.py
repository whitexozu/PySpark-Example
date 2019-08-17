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
    # reload(sys)
    # sys.setdefaultencoding('utf-8')

    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S%f')
    URL = 'http://localhost:8080/TA_SKM/ta/openApi/insertMntrLog.do'
    data = {'jobId': st, 'key': 'IV', 'type': 'M', 'state': 'S', 'serviceKey': 'b7a9dce1-9a39-4382-8b94-910220b5'}
    res = requests.post(URL, data=data)


    ef = executeFun()
    ss = ef.getSparkSession("insert ta_keyword_iv", sys.argv[1])

    campStartDt = sys.argv[2]
    insrcompCdArray = ['51']
    brchCdArray = ['51', '54', '55']
    spkCdArray = ['f', 'c', 'a']
    # callTypeArray = ['sb', 'nsb']

    createTableSql = []
    createTableSql.append("CREATE TABLE IF NOT EXISTS ta_keyword_iv ")
    createTableSql.append("( ")
    createTableSql.append("keyword string, ")
    createTableSql.append("iv string, ")
    createTableSql.append("user_count int ")
    createTableSql.append(") ")
    createTableSql.append("partitioned by (camp_start_dt string, insrcomp_cd string, brch_cd string, spk_cd string) ")
    createTableSql.append("ROW FORMAT DELIMITED ")
    # createTableSql.append("STORED AS ORC tblproperties(\"orc.compress\" = \"SNAPPY\") ")
    ss.sql(''.join(createTableSql))

    keyword = ''
    sbcpMax = 0
    nonsbcpMax = 0
    eventY = 0
    eventN = 0
    nonEventY = 0
    nonEventN = 0
    woeY = 0
    woeN = 0
    ivY = 0
    ivN = 0

    sf1 = StructField("keyword", StringType(), False)
    sf2 = StructField("iv", StringType(), False)
    sf3 = StructField("user_count", IntegerType(), False)
    sf4 = StructField("camp_start_dt", StringType(), False)
    sf5 = StructField("insrcomp_cd", StringType(), False)
    sf6 = StructField("brch_cd", StringType(), False)
    sf7 = StructField("spk_cd", StringType(), False)

    schema = StructType([sf1, sf2, sf3, sf4, sf5, sf6, sf7])

    selectQueryArray = []
    selectQueryArray.append("select count(distinct user_id) ")
    selectQueryArray.append("from ta_common_keyword ")
    selectQueryArray.append("where camp_start_dt='{0}' ")
    selectQueryArray.append("and insrcomp_cd='{1}' ")
    selectQueryArray.append("and call_type='{2}' ")
    selectQuery = (''.join(selectQueryArray)).format(campStartDt, insrcompCdArray[0], 'sb')
    sbTotalCountDf = ss.sql(selectQuery)
    for n in sbTotalCountDf.collect():
        sbcpMax = n[0]
        print(type(sbcpMax))
        print(sbcpMax)

    selectQueryArray = []
    selectQueryArray.append("select count(distinct user_id) ")
    selectQueryArray.append("from ta_common_keyword ")
    selectQueryArray.append("where camp_start_dt='{0}' ")
    selectQueryArray.append("and insrcomp_cd='{1}' ")
    selectQueryArray.append("and call_type='{2}' ")
    selectQuery = (''.join(selectQueryArray)).format(campStartDt, insrcompCdArray[0], 'nsb')
    nsbTotalCountDf = ss.sql(selectQuery)
    for n in nsbTotalCountDf.collect():
        nonsbcpMax = n[0]
        print(type(nonsbcpMax))
        print(nonsbcpMax)

    insertQueryArray = [];


    try :
        for insrcompCd in insrcompCdArray:
            for brchCd in brchCdArray:
                for spkCd in spkCdArray:
                    selectQuery = "select keyword, count(distinct user_id) as user_count from ta_common_keyword where camp_start_dt='{0}' and insrcomp_cd='{1}' and brch_cd='{2}' and spk_cd='{3}' and call_type='{4}' group by keyword".format(
                        campStartDt, insrcompCd, brchCd, spkCd, 'sb')
                    # print('selectQuery : ' + selectQuery)
                    sbDf = ss.sql(selectQuery)

                    selectQuery = "select keyword, count(distinct user_id) as user_count from ta_common_keyword where camp_start_dt='{0}' and insrcomp_cd='{1}' and brch_cd='{2}' and spk_cd='{3}' and call_type='{4}' group by keyword".format(
                        campStartDt, insrcompCd, brchCd, spkCd, 'nsb')
                    # print('selectQuery : ' + selectQuery)
                    nsbDf = ss.sql(selectQuery)

                    # print(sbDf.count())
                    # print(nsbDf.count())
                    if sbDf.count() > 0 and nsbDf.count() > 0 :
                        print('-----------------------------------')
                        # print('{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}|{8}|{9}|{10}|{11}|{12}|{13}|{14}|{15}'.format('keyword', 'campStartDt', 'insrcompCd', 'brchCd', 'spkCd', 'sbUserCount', 'nsbUserCount', 'Event(Y)', 'Event(N)', 'Non-Event(Y)', 'Non-Event(N)', 'WOE(=Y)', 'WOE(=N)', 'IV(=Y)', 'IV(=N)', 'IV Sum'))
                        resultDf = sbDf.join(nsbDf, 'keyword', 'inner')
                        insertRows = [];
                        for t in resultDf.collect():
                            try:
                                keyword = t[0]
                                eventY = float(t[1]) / float(sbcpMax)
                                eventN = float(float(sbcpMax) - float(t[1])) / float(sbcpMax)
                                nonEventY = float(t[2]) / float(nonsbcpMax)
                                nonEventN = float(float(nonsbcpMax) - float(t[2])) / float(nonsbcpMax)
                                woeY = math.log(eventY / nonEventY)
                                woeN = math.log(eventN / nonEventN)
                                ivY = woeY * (eventY - nonEventY)
                                ivN = woeN * (eventN - nonEventN)
                                # print('{0}, {1}, {2}, {3}, {4}'.format(keyword, campStartDt, insrcompCd, brchCd, spkCd))
                                # print('{0}, {1}'.format(ivY + ivN, t[1] + t[2]))
                                # print('{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}|{8}|{9}|{10}|{11}|{12}|{13}|{14}|{15}'.format(keyword.encode('utf-8'), campStartDt, insrcompCd, brchCd, spkCd, t[1], t[2], eventY, eventN, nonEventY, nonEventN, woeY, woeN, ivY, ivN, ivY + ivN))
                                insertRows.append([keyword, ivY + ivN, t[2], campStartDt, insrcompCd, brchCd, spkCd])
                            except ZeroDivisionError as e:
                                print(e)
                                pass
                            except ValueError as e:
                                print(e)
                                pass
                            except Exception as e:
                                print(e)
                                pass

                        insertDf = ss.createDataFrame(insertRows, schema)
                        # insertDf.write.mode("overwrite").partitionBy("camp_start_dt", "insrcomp_cd", "brch_cd", "spk_cd").format("orc").saveAsTable("ta_keyword_iv")
                        # ss.sql("show tables").show()
                        insertDf.write.format("orc").insertInto("ta_keyword_iv")

                        # insertQueryArray = []
                        # insertQueryArray.append("insert into table ta_keyword_iv partition (camp_start_dt = '{0}', insrcomp_cd = '{1}', brch_cd = '{2}', spk_cd = '{3}') values ".format(campStartDt, insrcompCd, brchCd, spkCd))
                        # for i, r in enumerate(insertRows):
                        #     if(i != 0):
                        #         insertQueryArray.append(",")
                        #     insertQueryArray.append("('{0}','{1}','{2}') ".format(r[0].encode('utf-8'), r[1], r[2]))
                        #
                        # print(''.join(insertQueryArray))
                        # ss.sql(insertQueryArray)

        data = {'jobId': st, 'key': 'IV', 'type': 'M', 'state': 'E', 'serviceKey': 'b7a9dce1-9a39-4382-8b94-910220b5'}
        res = requests.post(URL, data=data)
    except BaseException as e:
        print(e)
        data = {'jobId': st, 'key': 'IV', 'type': 'M', 'state': 'B', 'serviceKey': 'b7a9dce1-9a39-4382-8b94-910220b5'}
        res = requests.post(URL, data=data)
    finally:
        ss.stop()



# spark-submit createIVTable.py yarn