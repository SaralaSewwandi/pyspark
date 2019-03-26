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

response_row={}
response_row_list=[]

#request  ==> no of columns  22
for row in response_rows.take(response_rows.count()):
	#print(row[31])
	response_row['date']=row[3]
	response_row['time']=row[4]
	response_row['api_id']=row[16]
	response_row['api_name']=row[16]
	response_row['api_request_url']=row[21]
	response_row['api_response_status']=row[26]
	response_row['api_response_time']=row[30]
	response_row_list.append(response_row)
	
print("==========******** Response Columns ********=============")


for r in response_row_list:
	print(r)
