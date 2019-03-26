from pyspark import SparkContext, SparkConf

#create the spark context
conf = SparkConf().setAppName("MIFE Application").setMaster("local")
sc = SparkContext(conf=conf)

#read all lines from the log file
lines = sc.textFile("hdfs://sandbox:9000/user/root/bl/carbon.log")

#read INFO lines from all lines
info_lines = lines.filter(lambda line: "INFO" in line)

#========================== read request  lines ==========================

#read >>>> lines from INFO lines
request_lines = info_lines.filter(lambda line: ">>>>" in line)

#split the >>>> lines in to columns
request_rows =request_lines.map(lambda line : line.split(" "))

#-----last update line 2018-12-28 ---------
request_rows.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'))

#create a list for requests
#request_row_list=[]
#i=0

#for row in request_rows.take(request_rows.count()):
#	request_row=[row[3],row[4],row[16],row[16],row[21]]
#	request_row_list.insert(i,request_row)
#	i=i+1

print("============request list start =====")

#for i in range(len(request_row_list)):
#	print(request_row_list[i])

print("============request list end =======")

#print(len(request_row_list))

print("====================================")

#========================== read response lines ==========================

#read <<<< lines from INFO lines
#response_lines=info_lines.filter(lambda line: "<<<<" in line)

#split the <<<< lines in to columns
#response_rows=response_lines.map(lambda line:line.split(" "))

#create a list for  responses
#response_row_list=[]
#i=0
#for row in response_rows.take(response_rows.count()):
#	response_row=[row[3],row[4],row[16],row[16],row[21],row[26],row[30]]
#	response_row_list.insert(i,response_row)
#	i=i+1
	

#print("============response list start =====")

#for i in range(len(response_row_list)):
#	print(response_row_list[i])

#print("============response list end =======")

#print(len(response_row_list))

#print("=====================================")

