#-*- coding: utf-8 -*-

import collections

from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time


spark = SparkSession \
    .builder \
    .appName("sample") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "file:///Users/beginspark/Temp/") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

sc = spark.sparkContext

# 파이썬에서 데이터프레임 생성 시 네임드튜플(namedtuple), 튜플(tuple)
# Row, 커스텀 클래스(class), 딕셔너리(dictionary) 등을
# 사용하여 생성할 수 있다
tfidf = collections.namedtuple('tfidf', 'word tf idf user_count')

# sample dataframe 1
row1 = tfidf(word='보험', tf=22, idf=4, user_count=3)
row2 = tfidf(word='드리면서', tf=1, idf=1, user_count=1)
row3 = tfidf(word='의심', tf=3, idf=1, user_count=2)
row4 = tfidf(word='치료비가', tf=4, idf=2, user_count=1)
row5 = tfidf(word='오늘까지', tf=1, idf=1, user_count=1)
row6 = tfidf(word='아니에요', tf=2, idf=2, user_count=1)
row7 = tfidf(word='사', tf=1, idf=1, user_count=1)
row8 = tfidf(word='분쟁', tf=1, idf=1, user_count=1)
row9 = tfidf(word='같이', tf=1, idf=1, user_count=1)
row10 = tfidf(word='나간다고', tf=2, idf=1, user_count=1)
row11 = tfidf(word='확률이', tf=2, idf=2, user_count=1)
row12 = tfidf(word='드리', tf=1, idf=1, user_count=1)
row13 = tfidf(word='말씀드릴', tf=1, idf=1, user_count=1)
row14 = tfidf(word='하면', tf=2, idf=2, user_count=1)
row15 = tfidf(word='드린다고요', tf=1, idf=1, user_count=1)
row16 = tfidf(word='대개', tf=2, idf=2, user_count=1)
row17 = tfidf(word='때문에요', tf=2, idf=1, user_count=1)
row18 = tfidf(word='님', tf=3, idf=1, user_count=1)
row19 = tfidf(word='간단하게', tf=5, idf=3, user_count=2)
row20 = tfidf(word='것이십', tf=1, idf=1, user_count=1)
row21 = tfidf(word='같습니다', tf=1, idf=1, user_count=1)
row22 = tfidf(word='건으로', tf=1, idf=1, user_count=1)
row23 = tfidf(word='만원까지', tf=2, idf=1, user_count=1)
row24 = tfidf(word='거지', tf=1, idf=1, user_count=1)
row25 = tfidf(word='거기', tf=2, idf=2, user_count=1)
row26 = tfidf(word='설명서', tf=1, idf=1, user_count=1)
row27 = tfidf(word='들어갈거예요', tf=1, idf=1, user_count=1)
row28 = tfidf(word='천만원이고', tf=2, idf=2, user_count=1)
row29 = tfidf(word='이사', tf=1, idf=1, user_count=1)
row30 = tfidf(word='있어도', tf=1, idf=1, user_count=1)
row31 = tfidf(word='그니까', tf=2, idf=2, user_count=1)
row32 = tfidf(word='치료', tf=8, idf=3, user_count=2)
row33 = tfidf(word='받으시는', tf=8, idf=3, user_count=2)
row34 = tfidf(word='기간이', tf=2, idf=1, user_count=1)
row35 = tfidf(word='않고요', tf=1, idf=1, user_count=1)
row36 = tfidf(word='그동안', tf=1, idf=1, user_count=1)
row37 = tfidf(word='그럼', tf=6, idf=4, user_count=3)
row38 = tfidf(word='구천삼백', tf=2, idf=2, user_count=1)
row39 = tfidf(word='해봐야', tf=1, idf=1, user_count=1)

data = [row1, row2, row3, row4, row5, row6, row7, row8, row9, row10, row11, row12, row13, row14]
sample_df = spark.createDataFrame(data)
print('------------------------------------------------------------')
# print(sample_df.collect())
data3 = sample_df.rdd.map(lambda x: (x.word, x.tf, x.idf, x.user_count))
print(data3.collect())
for t in data3.collect():
    print(t[0], t[1])
print('------------------------------------------------------------')
