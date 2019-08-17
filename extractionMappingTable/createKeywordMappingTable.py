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

    ef = executeFun()
    ss = ef.getSparkSession("create ta_keyword_mapping table", sys.argv[1])

    campStartDt = '201709'

    createTableSql = []
    createTableSql.append("CREATE TABLE IF NOT EXISTS ta_keyword_mapping ")
    createTableSql.append("( ")
    createTableSql.append("keyword string, ")
    createTableSql.append("number int ")
    createTableSql.append(") ")
    createTableSql.append("partitioned by (camp_start_dt string) ")
    createTableSql.append("ROW FORMAT DELIMITED ")
    ss.sql(''.join(createTableSql))

    sf1 = StructField("keyword", StringType(), False)
    sf2 = StructField("number", IntegerType(), False)
    sf3 = StructField("camp_start_dt", StringType(), False)

    schema = StructType([sf1, sf2, sf3])

    try :
        selectQuery = "select keyword, row_number() over (order by keyword) as number from (select distinct keyword from ta_common_keyword where camp_start_dt = '{0}')".format(campStartDt)
        mappingDf = ss.sql(selectQuery)

        if mappingDf.count() > 0 :
            insertRows = []
            for t in mappingDf.collect():
                try:
                    if t[0] == None :
                        print("{0} {1}".format(t[0], "None"))
                        continue
                    elif t[0] == "" :
                        print("{0} {1}".format(t[0], "space"))
                    else :
                        insertRows.append([t[0], t[1], campStartDt])
                except ZeroDivisionError as e:
                    print(e)
                    pass
                except ValueError as e:
                    print(e)
                    pass

            insertDf = ss.createDataFrame(insertRows, schema)
            # insertDf.write.mode("overwrite").partitionBy("camp_start_dt").format("orc").saveAsTable("ta_keyword_mapping")
            insertDf.write.format("orc").insertInto("ta_keyword_mapping")
            # ss.sql("show tables").show()

    except BaseException as e:
        print(e)
    finally:
        ss.stop()



# spark-submit createKeywordMappingTable.py yarn