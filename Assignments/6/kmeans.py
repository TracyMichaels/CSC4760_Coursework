import matplotlib.animation as animation
import matplotlib.pyplot as plt
import numpy as np
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession
import matplotlib
import pandas

matplotlib.use('Webagg')
fig, ax = plt.subplots()

# create spark session
spark = SparkSession.builder.appName("K-means").getOrCreate()

# Loads data from local file
dataset = spark.read.format("libsvm").load("kmeans_input.txt")

# Trains a k-means model.
# number of clusters is k=2
kmeans = KMeans().setK(2).setSeed(1)
# fits model to dataset
model = kmeans.fit(dataset)

# Make predictions
# returns dataframe object
predictions = model.transform(dataset)
predictions.show()  # debug line

# validate predictions with silhouette score
silhouette = ClusteringEvaluator().evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# print centriods to console.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)


# get x and y coords of transformed dataset for plotting
x = predictions.select(predictions.columns[1]).rdd.map(
    lambda x: x[0][0]).collect()
y = predictions.select(predictions.columns[1]).rdd.map(
    lambda x: x[0][1]).collect()

# use the third column from preditions to set the label of each data point
label = predictions.select(
    predictions.columns[2]).rdd.map(lambda x: x).collect()


# plot dataset labeled by predictions
plt.scatter(x, y, c=label)

# plot centroids
plt.scatter(centers[0][0], centers[0][1])
plt.scatter(centers[1][0], centers[1][1])

# add visual representation of silhouette score to plot
cir1 = plt.Circle((centers[0][0], centers[0][1]),
                  silhouette, color='c', fill=False)
cir2 = plt.Circle((centers[1][0], centers[1][1]),
                  silhouette, color='tab:orange', fill=False)
ax.add_patch(cir1)
ax.add_patch(cir2)

# normalize grid to be square
ax.set_aspect('equal', adjustable='datalim')

# display plot
plt.show()
