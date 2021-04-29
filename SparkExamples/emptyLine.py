import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

RDD1 = sc.textFile("/home/rob/data/log.txt")
print(RDD1.collect())

# Create Accumulator[Int] initialized to 0
blankLines = sc.accumulator(0)

def emptyLine(line):
    global blankLines  # Make the global variable accessible
    if (line == ""):
        blankLines += 1
        return False
    return True

RDD2 = RDD1.filter(emptyLine)
print(RDD2.collect())

print("Blank lines: %d" % blankLines.value)