import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

RDD1 = sc.textFile("/home/rob/data/log.txt")
print(RDD1.collect())

# Create Accumulator[Int] initialized to 0
errorLines = sc.accumulator(0)
nonErrorLines = sc.accumulator(0)

def errorSign(line):
    global errorLines, nonErrorLines  # Make the global variables accessible
    if line.startswith('ERROR:'):
        errorLines += 1
        return False
    else:
        nonErrorLines += 1
        return True

RDD2 = RDD1.filter(errorSign)
print(RDD2.collect())

print("error lines: %d" % errorLines.value)
print("non-error lines: %d" % nonErrorLines.value)
