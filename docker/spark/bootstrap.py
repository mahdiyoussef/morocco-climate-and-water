from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('resolve-platform-jars').getOrCreate()
assert spark.range(1).count() == 1
spark.stop()
