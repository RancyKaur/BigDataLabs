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
print(lines.take(5))
squares = rdd.map(lambda x: x*x)
print(squares.take(5))
lines = lines.map(lambda l: l.split(','))


# while flatMap() can produce multiple output elements for each input element.
cubeSquare = rdd.flatMap(lambda x: [x*x, x*x*x] if x%2==1 else [])
print(cubeSquare.take(5))