import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf

# creates spark context object
conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# load json files into DataFrame objects
sqlContext = pyspark.SQLContext(sc)
dfCityStateMap = sqlContext.read.json("cityStateMap.json")
dfTweets = sqlContext.read.json("tweets.json")

# for report screenshot
# dfTweets.show()
# dfCityStateMap.show()

# merge the two DataFrames so that each geo location is matched with it's corresponding
# city and state
dfResult = dfTweets.join(dfCityStateMap, (dfTweets.geo == dfCityStateMap.city))

# to show intermediate step for report screenshot
# dfResult.show()

# groups by the state and counts the number of each instance
# and outputs the result
dfResult.groupby('state').count().show()
