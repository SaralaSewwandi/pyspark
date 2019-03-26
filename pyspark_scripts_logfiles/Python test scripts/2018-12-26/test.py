from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

# spark is an existing SparkSession
df = spark.read.json("customers.json")
# Displays the content of the DataFrame to stdout
df.show()
