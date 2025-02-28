from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Load 1PB Data") \
    .config("spark.sql.files.maxPartitionBytes", "512MB") \
    .getOrCreate()

df = spark.read.parquet("s3://franhan-bucket/dados-1pb/") 
df.cache()
df.show(5)
