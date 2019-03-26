from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import split, regexp_extract

conf = SparkConf().setAppName("MIFE Application").setMaster("local")
sc = SparkContext(conf=conf)


sqlContext = SQLContext(sc)

log_file = sqlContext.read.text( "hdfs://sandbox:9000/user/root/bl/carbon.log")


rows=log_file.map(lambda line:line.split(" "))

for row in rows.take(rows.count()):
	print(row[0])

