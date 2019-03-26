from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import split, regexp_extract
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

lines=spark.read.text("/home/bl/carbon.log")


info_lines=lines.filter(lines.value.contains("INFO"))


print("=================")

info_lines.show(50, truncate=False)


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

