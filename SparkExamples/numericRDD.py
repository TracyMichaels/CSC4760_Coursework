import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf
import math

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

distances = sc.textFile("/home/rob/data/numericValues.txt")

# Convert our RDD of strings to numeric data so we can compute stats and
# remove the outliers.
distanceNumerics = distances.map(lambda string: float(string))
stats = distanceNumerics.stats()
print(stats)
stddev = stats.stdev()
mean = stats.mean()
reasonableDistances = distanceNumerics.filter(
    lambda x: math.fabs(x - mean) < 3 * stddev)
print(reasonableDistances.collect())
