from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    sc = SparkContext("local[2]", appName="Tweet Streaming")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 1)

    lines = ssc.socketTextStream("localhost", 49742)
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
