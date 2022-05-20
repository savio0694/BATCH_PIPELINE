from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Final_demo').getOrCreate()

from pyspark.sql.functions import *
data = spark.read.json("gs://landingbucket0694/test_file.json")

data=data.withColumn("sales",col("sale_amt")*col('qty'))
data.repartition(1).write.mode("overwrite").parquet("gs://cleanbucket0694/CLEAN")