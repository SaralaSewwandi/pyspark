from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import split, regexp_extract
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

#create spark session
spark = SparkSession.builder.getOrCreate()

#read from the log file
lines=spark.read.text("/home/bl/carbon.log")

#read  INFO lines from all lines
info_lines=lines.filter(lines.value.contains("INFO"))

#read >>>> lines from the info lines
request_lines=info_lines.filter(info_lines.value.contains(">>>>"))

request_rows=request_lines.select(split(request_lines['value']," ").alias('cols'))

print("=================start request data===============================")

#create intermediate data frame
temp_df=request_rows.select(regexp_extract(request_rows.cols[3],r'\d\d\d\d-\d\d-\d\d',0).alias('date'),regexp_extract(request_rows.cols[4],r'\d\d:\d\d:\d\d',0).alias('time'),request_rows.cols[16].alias('api_info'),request_rows.cols[21].alias('api_request_url'))

#create request data frame
request_df=temp_df.select(temp_df['date'],temp_df['time'],split(temp_df['api_info'],"""\|""")[0].alias('api_id'),split(temp_df['api_info'],"""\|""")[1].alias('api_name'),split(temp_df['api_request_url'],",")[0].alias('request_url'))

request_df.show(50,truncate=False)
print(request_df.count())

print("==================end request data ==========")


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

