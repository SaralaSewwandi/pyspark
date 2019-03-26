from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import split, regexp_extract

conf = SparkConf().setAppName("PYTHON Test Application").setMaster("local")
sc = SparkContext(conf=conf)

baby_names = sc.textFile("hdfs://sandbox:9000/user/root/bl/baby_names.csv")

print("====================================")

print("====count======")
print(baby_names.count())
print("=====first=====")
print(baby_names.first())


print("=============DPI Usage End==========")

linesWithDominic = baby_names.filter(baby_names.contains("DOMINIC"))
print(linesWithDominic)

print("===================================")
