from pyspark import SparkContext
sc = SparkContext("local", "count app")

df = spark.read.text("carbon.log")

print(df.count())  # Number of rows in this DataFrame
