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

    # ts = time.time()
    # st = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S%f')
    # URL = 'http://localhost:8080/TA_SKM/ta/openApi/insertMntrLog.do'
    # data = {'jobId': st, 'key': 'IV', 'type': 'M', 'state': 'S', 'serviceKey': 'b7a9dce1-9a39-4382-8b94-910220b5'}
    # res = requests.post(URL, data=data)


    ef = executeFun()
    ss = ef.getSparkSession("insert ta_customer_iptc", sys.argv[1])

    campStartDt = '201709'
    insrcompCdArray = ['51']
    brchCdArray = ['51', '54', '55']
    talkTimeCdArray = ['a', 't1', 't2', 't3', 't4']

    createTableSql = []
    createTableSql.append("CREATE TABLE IF NOT EXISTS ta_customer_iptc ")
    createTableSql.append("( ")
    createTableSql.append("ql int, ")
    createTableSql.append("rc string, ")
    createTableSql.append("mm string, ")
    createTableSql.append("rnum int, ")
    createTableSql.append("iptc double ")
    createTableSql.append(") ")
    createTableSql.append("partitioned by (camp_start_dt string, insrcomp_cd string, brch_cd string, talk_time_cd string) ")
    createTableSql.append("ROW FORMAT DELIMITED ")
    # createTableSql.append("STORED AS ORC tblproperties(\"orc.compress\" = \"SNAPPY\") ")
    ss.sql(''.join(createTableSql))

    sf1 = StructField("ql", StringType(), False)
    sf2 = StructField("rc", StringType(), False)
    sf3 = StructField("mm", StringType(), False)
    sf4 = StructField("rnum", StringType(), False)
    sf5 = StructField("iptc", DoubleType(), False)
    sf6 = StructField("camp_start_dt", StringType(), False)
    sf7 = StructField("insrcomp_cd", StringType(), False)
    sf8 = StructField("brch_cd", StringType(), False)
    sf9 = StructField("talk_time_cd", StringType(), False)

    schema = StructType([sf1, sf2, sf3, sf4, sf5, sf6, sf7, sf8, sf9])

    selectQueryArray = []
    insertQueryArray = []

    try :
        for insrcompCd in insrcompCdArray:
            for brchCd in brchCdArray:
                for talkTimeCd in talkTimeCdArray:
                    selectQueryArray = []
                    selectQueryArray.append("with qt ( ")
                    selectQueryArray.append("    select '1' as rnum, '1' as ql ")
                    selectQueryArray.append("    union all  ")
                    selectQueryArray.append("    select '1' as rnum, '2' as ql ")
                    selectQueryArray.append("    union all  ")
                    selectQueryArray.append("    select '1' as rnum, '3' as ql ")
                    selectQueryArray.append("    union all  ")
                    selectQueryArray.append("    select '1' as rnum, '4' as ql ")
                    selectQueryArray.append("    union all  ")
                    selectQueryArray.append("    select '1' as rnum, '5' as ql ")
                    selectQueryArray.append("), rt ( ")
                    selectQueryArray.append("    select '1' as rnum, '01' as rc ")
                    selectQueryArray.append("    union all  ")
                    selectQueryArray.append("    select '1' as rnum, '02' as rc ")
                    selectQueryArray.append("    union all  ")
                    selectQueryArray.append("    select '1' as rnum, '04' as rc ")
                    selectQueryArray.append("), mt ( ")
                    selectQueryArray.append("    select '1' as rnum, 'min' as mm ")
                    selectQueryArray.append("    union all  ")
                    selectQueryArray.append("    select '1' as rnum, 'max' as mm ")
                    selectQueryArray.append("), qrmt ( ")
                    selectQueryArray.append("    select ")
                    selectQueryArray.append("        ql, rc, mm ")
                    selectQueryArray.append("    from qt ")
                    selectQueryArray.append("    cross join rt on qt.rnum = rt.rnum ")
                    selectQueryArray.append("    cross join mt on qt.rnum = mt.rnum ")
                    selectQueryArray.append("), dt ( ")
                    selectQueryArray.append("    select ")
                    selectQueryArray.append("        callRsltCd, ")
                    selectQueryArray.append("        iptc, ")
                    selectQueryArray.append("        row_number() over (partition by callRsltCd order by iptc desc) as rnum ")
                    selectQueryArray.append("    from ( ")
                    selectQueryArray.append("        select ")
                    selectQueryArray.append("            if(call_rslt_cd == '03', '02', call_rslt_cd) as callRsltCd, ")
                    selectQueryArray.append("            call_time_c / (call_time_c + call_time_f) * 100 as iptc ")
                    selectQueryArray.append("        from ta_common_user ")
                    selectQueryArray.append("        where camp_start_dt = '{0}' ")
                    selectQueryArray.append("        and insrcomp_cd = '{1}' ")
                    selectQueryArray.append("        and brch_cd = '{2}' ")
                    if talkTimeCd == 't1':
                        selectQueryArray.append("        and call_time_c + call_time_f < 12000 ")
                    elif talkTimeCd == 't2':
                        selectQueryArray.append("        and call_time_c + call_time_f >= 12000 and call_time_c + call_time_f < 18000 ")
                    elif talkTimeCd == 't3':
                        selectQueryArray.append("        and call_time_c + call_time_f >= 18000 and call_time_c + call_time_f < 30000 ")
                    elif talkTimeCd == 't4':
                        selectQueryArray.append("        and call_time_c + call_time_f >= 30000 ")
                    selectQueryArray.append("    ) ")
                    selectQueryArray.append("), dc ( ")
                    selectQueryArray.append("    select  ")
                    selectQueryArray.append("        callRsltCd, ")
                    selectQueryArray.append("        count(*) as cntMax, ")
                    selectQueryArray.append("        int(round(count(*) / (select count(*) from qt))) as cntDiv ")
                    selectQueryArray.append("    from dt ")
                    selectQueryArray.append("    group by callRsltCd ")
                    selectQueryArray.append("), tt ( ")
                    selectQueryArray.append("    select ")
                    selectQueryArray.append("        ql, rc, mm, int(rnum) as rnum ")
                    selectQueryArray.append("    from ( ")
                    selectQueryArray.append("        select ")
                    selectQueryArray.append("            ql, ")
                    selectQueryArray.append("            rc, ")
                    selectQueryArray.append("            mm, ")
                    selectQueryArray.append("            case ")
                    selectQueryArray.append("                when ql = (select count(*) from qt)  ")
                    selectQueryArray.append("                    then ")
                    selectQueryArray.append("                        case  ")
                    selectQueryArray.append("when rc = '01' and mm = 'min' then (ql - 1) * (select cntDiv from dc where callRsltCd = '01') + 1 ")
                    selectQueryArray.append("                        when rc = '01' and mm = 'max' then (select cntMax from dc where callRsltCd = '01') ")
                    selectQueryArray.append("when rc = '02' and mm = 'min' then (ql - 1) * (select cntDiv from dc where callRsltCd = '02') + 1 ")
                    selectQueryArray.append("                        when rc = '02' and mm = 'max' then (select cntMax from dc where callRsltCd = '02') ")
                    selectQueryArray.append("when rc = '04' and mm = 'min' then (ql - 1) * (select cntDiv from dc where callRsltCd = '04') + 1 ")
                    selectQueryArray.append("                        when rc = '04' and mm = 'max' then (select cntMax from dc where callRsltCd = '04') ")
                    selectQueryArray.append("                        else 0 end ")
                    selectQueryArray.append("                else ")
                    selectQueryArray.append("                    case ")
                    selectQueryArray.append("when rc = '01' and mm = 'min' then (ql - 1) * (select cntDiv from dc where callRsltCd = '01') + 1 ")
                    selectQueryArray.append("                    when rc = '01' and mm = 'max' then ql * (select cntDiv from dc where callRsltCd = '01') ")
                    selectQueryArray.append("when rc = '02' and mm = 'min' then (ql - 1) * (select cntDiv from dc where callRsltCd = '02') + 1 ")
                    selectQueryArray.append("                    when rc = '02' and mm = 'max' then ql * (select cntDiv from dc where callRsltCd = '02') ")
                    selectQueryArray.append("when rc = '04' and mm = 'min' then (ql - 1) * (select cntDiv from dc where callRsltCd = '04') + 1 ")
                    selectQueryArray.append("                    when rc = '04' and mm = 'max' then ql * (select cntDiv from dc where callRsltCd = '04') ")
                    selectQueryArray.append("                    else 0 end ")
                    selectQueryArray.append("                end as rnum ")
                    selectQueryArray.append("        from qrmt ")
                    selectQueryArray.append("        order by ql, rc, mm ")
                    selectQueryArray.append("    ) ")
                    selectQueryArray.append(") ")
                    selectQueryArray.append("select tt.ql, tt.rc, tt.mm, if(tt.rnum is null, 0, tt.rnum), if(dt.iptc is null, 0, dt.iptc)  ")
                    selectQueryArray.append("from tt cross join dt on tt.rc = dt.callRsltCd and tt.rnum = dt.rnum ")
                    selectQueryArray.append("order by tt.ql, tt.rc, tt.mm ")

                    selectQuery = (''.join(selectQueryArray)).format(campStartDt, insrcompCd, brchCd)

                    resultDf = ss.sql(selectQuery)

                    print(resultDf.count())
                    if resultDf.count() > 0 :
                        print('-----------------------------------')
                        # print('{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}|{8}|{9}|{10}|{11}|{12}|{13}|{14}|{15}'.format('keyword', 'campStartDt', 'insrcompCd', 'brchCd', 'spkCd', 'sbUserCount', 'nsbUserCount', 'Event(Y)', 'Event(N)', 'Non-Event(Y)', 'Non-Event(N)', 'WOE(=Y)', 'WOE(=N)', 'IV(=Y)', 'IV(=N)', 'IV Sum'))
                        insertRows = []
                        for t in resultDf.collect():
                            try:
                                insertRows.append([t[0], t[1], t[2], t[3], t[4], campStartDt, insrcompCd, brchCd, talkTimeCd])
                            except ZeroDivisionError as e:
                                print(e)
                                pass
                            except ValueError as e:
                                print(e)
                                pass

                        insertDf = ss.createDataFrame(insertRows, schema)
                        insertDf.write.format("orc").insertInto("ta_customer_iptc")

        # data = {'jobId': st, 'key': 'IV', 'type': 'M', 'state': 'E', 'serviceKey': 'b7a9dce1-9a39-4382-8b94-910220b5'}
        # res = requests.post(URL, data=data)
    except BaseException as e:
        print(e)
        # data = {'jobId': st, 'key': 'IV', 'type': 'M', 'state': 'B', 'serviceKey': 'b7a9dce1-9a39-4382-8b94-910220b5'}
        # res = requests.post(URL, data=data)
    finally:
        ss.stop()



# spark-submit createIVTable.py yarn