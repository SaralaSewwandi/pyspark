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

info_lines = lines.filter(lambda line: "INFO" in line)

request_lines=info_lines.filter(lambda line: ">>>>" in line)

request_rows=request_lines.map(lambda line:line.split(" "))

#info_rows=info_lines.map(lambda line:line.split(" "))

print("==========================")

#request  ==> no of columns  22
for row in request_rows.take(request_rows.count()):print(row[22])

print("============================")


