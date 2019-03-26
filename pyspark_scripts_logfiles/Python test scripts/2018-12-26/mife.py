from pyspark import SparkContext, SparkConf


conf = SparkConf().setAppName("MIFE Application").setMaster("local")
sc = SparkContext(conf=conf)

print("=====================================")

print(sc)
print(sc.version)


print("=====================================")
lines = sc.textFile("hdfs://sandbox:9000/user/root/bl/carbon.log")


#firstLine=lines.first()
#lineLengths = lines.map(lambda s: len(s))
#totalLength = lineLengths.reduce(lambda a, b: a + b)

print("===============first line==========")
#print(totalLength)
#print(firstLine)


print("==========RID Lines=========")

linesWithRID_count = lines.filter(lambda line: "DPIUsageInfoAPI" in line).count()

linesWithRID = lines.filter(lambda line: "DPIUsageInfoAPI" in line)
print("*****************************")
print(linesWithRID_count)
#print(linesWithRID.take(5))
print(linesWithRID.collect())

allLines=linesWithRID.collect()

rdd=sc.parallelize(allLines)

rdd.saveAsTextFile("hdfs://sandbox:9000/user/root/bl/all.txt")


print("=================print msg===============")

print("****************printed*********")

print("================================")

