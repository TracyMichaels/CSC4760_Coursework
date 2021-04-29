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

def lookupCountryName(countryCode):
    for oneCode in codeCountryList:
        if oneCode['code'] == countryCode:
            return oneCode['country']
            break

DF1 = spark.read.json("/home/rob/data/phoneBook.json")
DF1.show()

json_data = open("/home/rob/data/coutryLookupTable.json").read()
codeCountryList = json.loads(json_data)

RDD1 = DF1.rdd
RDD2 = RDD1.map(lambda attributes: Row(name=attributes[0], \
                                       phone=attributes[1], \
                                       country=lookupCountryName(attributes[1].split("-")[0])))
DF2 = RDD2.toDF()
DF2.show()

DF2.write.json("/home/rob/data/phoneBook_naive.jsonl")
