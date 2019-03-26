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

#linesWithRID_count = lines.filter(lambda line: "DPIUsageInfoAPI" in line).count()
#===== INFO Response Columns==========
info_lines = lines.filter(lambda line: "INFO" in line)

response_lines=info_lines.filter(lambda line: "<<<<" in line)

response_rows=response_lines.map(lambda line:line.split(" "))

#info_rows=info_lines.map(lambda line:line.split(" "))

print("==========================")


response_row_list=[]

i=0
#request  ==> no of columns  22
for row in response_rows.take(response_rows.count()):
#row=response_rows.first()
	#print(row[31])
	response_row=[row[3],row[4],row[16],row[16],row[21],row[26],row[30]]
	response_row_list.insert(i,response_row)
	i=i+1
	
	
print("==========******** Response Columns ********=============")

print("////////// result //////////")


#for i in range(len(response_row_list)):
#	for j in range(7):
#		print(response_row_list[i][j])

#print(response_row)
#print(len(response_row_list))
print(";;;;;;;;;;;;;;;;;;;;;;;;;;;;;")

#print(len(response_row_list))
print(";;;;;;;;;;;;;;;;;;;;;;;;;;;")

for i in range(len(response_row_list)):
	print(response_row_list[i])

print(";;;;;;;;;;;;;;;;;;;;;;;;;;;")

print(len(response_row_list))