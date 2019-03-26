from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import split, regexp_extract
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

#create spark session
spark = SparkSession.builder.getOrCreate()

#read from the log file
lines=spark.read.text("/home/bl/carbon.log")

#read  ERROR lines from all lines
error_lines=lines.filter(lines.value.contains("ERROR"))


error_rows=error_lines.select(split(error_lines['value'],"} -").alias('error_cols'))

print("=================start error data===============================")

#create intermediate data frame
temp_df=error_rows.select(error_rows.error_cols[0].alias('left'),error_rows.error_cols[1].alias('right'))

#create error data frame
error_df=temp_df.select(regexp_extract(split(temp_df['left']," ")[3],r'\d\d\d\d-\d\d-\d\d',0).alias('date'),regexp_extract(split(temp_df['left']," ")[4],r'\d\d:\d\d:\d\d',0).alias('time'),temp_df['right'].alias('description'))

error_df.show(50,truncate=False)
print(error_df.count())

print("==================end error data ==========")


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

