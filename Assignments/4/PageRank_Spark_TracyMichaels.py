import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf

def getContributions(list, pr):
    for key in list:
        yield(key, pr/len(list))

conf = SparkConf()
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Load the adjacency list file
AdjList1 = sc.textFile("/home/tracy/assignment4/02AdjacencyList.txt")
print(AdjList1.collect())

AdjList2 = AdjList1.map(lambda line : [int(x) for x in line.split(" ")])  # 1. Replace the lambda function with yours
AdjList3 = AdjList2.map(lambda x : [x[0], list(x[1:])])  # 2. Replace the lambda function with yours
AdjList3.persist()
print(AdjList3.collect())

nNumOfNodes = AdjList3.count()
print("Total Number of nodes")
print(nNumOfNodes)

# Initialize each page's rank; since we use mapValues, the resulting RDD will have the same partitioner as links
print("Initialization")
PageRankValues = AdjList3.mapValues(lambda v : 1/nNumOfNodes)  # 3. Replace the lambda function with yours
print(PageRankValues.collect())

# Run 30 iterations
print("Run 30 Iterations")
for i in range(1, 30):
    print("Number of Iterations")
    print(i)
    JoinRDD = AdjList3.join(PageRankValues)
    print("join results")
    print(JoinRDD.collect())
    contributions = JoinRDD.flatMap(lambda x : getContributions(x[1][0], x[1][1]))  # 4. Replace the lambda function with yours
    print("contributions")
    print(contributions.collect())
    accumulations = contributions.reduceByKey(lambda x, y : x + y)  # 5. Replace the lambda function with yours
    print("accumulations")
    print(accumulations.collect())
    PageRankValues = accumulations.mapValues(lambda v : (v*.85) + (.15/nNumOfNodes))  # 6. Replace the lambda function with yours
    print("PageRankValues")
    print(PageRankValues.collect())

print("=== Final PageRankValues ===")
print(PageRankValues.collect())

# Write out the final ranks
PageRankValues.coalesce(1).saveAsTextFile("/home/tracy/assignment4/PageRankValues_Final")
