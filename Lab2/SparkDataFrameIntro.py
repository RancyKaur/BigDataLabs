from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def init_spark():
    spark = SparkSession.builder.appName("DF Frame")\
            .config("spark.some.config.option", "some-value")\
            .getOrCreate()
    return spark

spark = init_spark()
filename = "trees2016.csv"
df = spark.read.csv(filename, header=True, mode="DROPMALFORMED")
df = df.where(col("Nom_parc").isNotNull())
print(df.take(5))
df.printSchema()
