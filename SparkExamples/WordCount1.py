import pyspark
import sys
from pyspark.context import SparkContext
from pyspark import SparkConf

# Parse the input parameters
input_file_name = sys.argv[1]
number_k_for_topK = int(sys.argv[2])
print input_file_name + " " + str(number_k_for_topK)

# Prepare the Spark context
conf = SparkConf().setMaster("local") \
                  .setAppName("Word Count Spark") \
                  .set("spark.executor.memory", "4g") \
                  .set("spark.executor.instances", 1)
sc = SparkContext(conf = conf)
book = sc.textFile(input_file_name)

# WordCount
words_counted = book.flatMap(lambda line: line.split(" ")) \
                    .map(lambda word:(word,1)) \
                    .reduceByKey(lambda x,y:x+y )

# Output the top-K most frequent words
print words_counted.sortBy(lambda (key,value) : -value).take(number_k_for_topK)
