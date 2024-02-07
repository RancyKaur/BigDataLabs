import csv
import os
import sys
# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

from csv import reader
def init_spark():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession.builder.appName("Python SPARK basic example")\
        .config("spark.some.config.option", "some-value")\
        .getOrCreate()
    return spark

spark = init_spark()
filename = "trees2016.csv"
lines = spark.sparkContext.textFile(filename) # one way to create rdd from file
data = [i for i in range(10)]
rdd = spark.sparkContext.parallelize(data) #other way to create rdd is using parallelize

#### Transformation Methods

# 1. map - Map() is one-to-one, while flatMap() is one-to-many. This means that map() will produce a single output element for each input element,
squares = rdd.map(lambda x: x*x)

# while flatMap() can produce multiple output elements for each input element.
cubeSquare = rdd.flatMap(lambda x: [x*x, x*x*x] if x%2==1 else [])


# filter(func) : Return a new dataset formed by selecting those elements of the source on which func returns true.
oddnumbers = rdd.filter(lambda x: x%3 ==1 )

# reduceByKey(func) : When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function func, which must be of type (V,V) => V.

data = [(i%3, i) for i in range(10)]
#print(data)
rdd1 = spark.sparkContext.parallelize(data)
op = rdd1.reduceByKey(lambda x,y: x+y)
#print(op.collect())

# intersection(dataset) : Return a new RDD that contains the intersection of elements in the source dataset and the argument.
data1 = [i for i in range(10)]
data2 = [i for i in range(5,15)]
rdd1 = spark.sparkContext.parallelize(data1)
rdd2 = spark.sparkContext.parallelize(data2)
rdd1 = rdd1.intersection(rdd2)
#print(rdd1.collect())

#union(funct): Return a new dataset that contains the union of the elements in the source dataset and the argument.
data1 = [i for i in range(10)]
data2 = [i for i in range(5,15)]
rdd1 = spark.sparkContext.parallelize(data1)
rdd2 = spark.sparkContext.parallelize(data2)
rdd1 = rdd1.union(rdd2)
#print(rdd1.collect())

#distinct() : Return a new dataset that contains the distinct elements of the source dataset.
data = [i%3 for i in range(10)]
rdd = spark.sparkContext.parallelize(data)
rdd = rdd.distinct()
#print(rdd.collect())

#zipWithIndex() : Assign an index to each element in the RDD.
data = [i for i in range(10)]
rdd = spark.sparkContext.parallelize(data)
rdd = rdd.zipWithIndex()
#print(rdd.collect())

# groupByKey() : When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable) pairs.
data = [(i%3, i) for i in range(10)]
rdd = spark.sparkContext.parallelize(data)
rdd1 = rdd.groupByKey()
print(rdd.collect())
print(rdd1.collect())