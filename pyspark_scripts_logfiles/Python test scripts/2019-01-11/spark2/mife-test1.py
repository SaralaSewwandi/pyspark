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

print("=================")

#df=request_rows.select(regexp_extract(request_rows.cols[3],r'\d\d\d\d-\d\d-\d\d',0).alias('date'),regexp_extract(request_rows.cols[4],r'\d\d:\d\d:\d\d',0).alias('time'),)

api_info_df=request_rows.select(request_rows.cols[16].alias('api_info'))


api_info_rows_df=api_info_df.select(split(api_info_df['api_info'],"""\|""").alias('api_info_cols'))

api_id_df=api_info_rows_df.select(api_info_rows_df.api_info_cols[0].alias('api_id'))

api_name_df=api_info_rows_df.select(api_info_rows_df.api_info_cols[1].alias('api_name'))

api_request_url_df=request_rows.select(request_rows.cols[21].alias('api_request_url'))

#create final data frame
df1=request_rows.select(regexp_extract(request_rows.cols[3],r'\d\d\d\d-\d\d-\d\d',0).alias('date'),regexp_extract(request_rows.cols[4],r'\d\d:\d\d:\d\d',0).alias('time'),request_rows.cols[16].alias('api_info'),request_rows.cols[21].alias('api_request_url'))

df2=df1.select(df1['date'],df1['time'],split(df1['api_info'],"""\|""")[0].alias('api_id'),split(df1['api_info'],"""\|""")[1].alias('api_name'),df1['api_request_url'])

#df=date_df.withColumn("TIME",time_df.time)

#api_response_status=request_rows.select(request_rows.cols[26].alias('api_response_status'))
#api_response_time=request_rows.select(request_rows.cols[30].alias('api_response_time'))

#date_list=date_df.collect()
#time_list=time_df.collect()
#api_id_list=api_id_df.collect()
#api_name_list=api_name_df.collect()
#api_request_url_list=api_request_url_df.collect()



#print(date_list)
#print(type(date_list))
df2.show(50,truncate=False)
print(df2.count())
#api_request_url_df.printSchema()
#print(type(api_id))
#print(type(api_id['api_id']))
print("==================")


#read >>>> lines from  INFO lines
#request_lines=info_lines.filter(">>>>")

#request_lines.show(10)

#log_file.show(5)

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

