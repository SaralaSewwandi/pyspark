from pyspark.sql import SparkSession

from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import split, regexp_extract

#create the spark context
conf = SparkConf().setAppName("MIFE Application").setMaster("local")
sc = SparkContext(conf=conf)

spark=SparkSession(sc)

# spark is an existing SparkSession
df = spark.read.json("customers.json")
# Displays the content of the DataFrame to stdout
df.show()
