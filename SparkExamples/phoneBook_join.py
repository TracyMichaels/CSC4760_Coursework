import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import *
import json
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession \
    .builder \
    .appName("Phone Book - Country Look up") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

DF1 = spark.read.json("/home/rob/Accum_Broadcast/phoneBook.json")
DF3 = spark.read.json("/home/rob/Accum_Broadcast/coutryLookupTable.json")
DF1.show()

RDD1 = DF1.rdd
RDD2 = RDD1.map(lambda attributes: Row(name=attributes[0], \
                                       phone=attributes[1], \
                                       code=attributes[1].split("-")[0]))
DF2 = RDD2.toDF()
DF2.show()

DF4_joined = DF2.join(DF3, DF2.code == DF3.code, 'inner').drop(DF3.code)
DF4_joined.show()

DF4_joined.write.json("/home/rob/Accum_Broadcast/phoneBook_join.jsonl")
